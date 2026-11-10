import { randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { chain } from "stream-chain";
import { parser } from "stream-json";
import { ignore } from "stream-json/filters/ignore.js";
import { pick } from "stream-json/filters/pick.js";
import { streamArray } from "stream-json/streamers/stream-array.js";
import { streamObject } from "stream-json/streamers/stream-object.js";

type RecordValue = Record<string, unknown>;
type CleanupOptions = {
  removeEmptyFlights: boolean;
  mergeByDays: boolean;
  mergeGapDays: number;
  splitByDays: boolean;
  splitGapDays: number;
  removeSmallAmplitude: boolean;
  removeWorseningQa: boolean;
  mergeKnownTailVariants: boolean;
  removeJunkAircraft: boolean;
  applyDateCorrections: boolean;
  removeTargetedTests: boolean;
  mergeEvidenceBasedChains: boolean;
};
type Counts = {
  aircraft: number;
  tests: number;
  flights: number;
  balanceReadings: number;
  trackReadings: number;
  fftCaptures: number;
  fftSamples: number;
};
type BalanceReading = {
  testCondition: string;
  part: string;
  axis: string;
  amplitude: number | null;
  phase: number | null;
};
type FlightReading = {
  tailNumber: string;
  testNumber: string;
  flightNumber: string;
  started: string | null;
  ended: string | null;
  balanceData: BalanceReading[];
  trackData: RecordValue[];
};
type HistogramData = {
  allFlightsPerTest: number[];
  hauptFlightsPerTest: number[];
  heckFlightsPerTest: number[];
  hauptBodenlaufPerTest: number[];
  hauptOtherConditionsPerTest: number[];
  heckBodenlaufPerTest: number[];
  heckOtherConditionsPerTest: number[];
};
type Analytics = {
  counts: Counts;
  byCondition: Array<{ name: string; flights: number }>;
  byPart: Array<{ name: string; readings: number }>;
  compliance: Array<{ axis: string; limit: number; violations: number }>;
  histograms: HistogramData;
  averageFlightsPerTest: number;
};
type FftFinding = {
  code: string;
  label: string;
  count: number;
  severity: "info" | "warning" | "critical";
};
type FftAudit = {
  captures: number;
  samples: number;
  findings: FftFinding[];
  preserved: boolean;
};
type Session = {
  id: string;
  directory: string;
  rawPath: string;
  previewPath?: string;
  cleanedPath?: string;
  filename: string;
  bytes: number;
  importedAt: string;
  expiresAt: number;
  counts: Counts;
  log: string[];
  flights: FlightReading[];
  analytics: Analytics;
  fftAudit: FftAudit;
  preview?: RecordValue;
  options?: CleanupOptions;
  separatedTests: unknown[];
};

const sessions = new Map<string, Session>();
const SESSION_TTL_MS = 30 * 60 * 1000;
const MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024;
const MAX_FLIGHT_ROWS = 100_000;
const PAIRS: Array<[string, string]> = [
  ["84+39", "8439"],
  ["84+43", "8443"],
  ["84+91", "8491"],
  ["84+97", "8497"],
  ["85+01", "8501"],
  ["85+03", "8503"],
];
const digits = (value: string) => value.replace(/\D/g, "");
const pairByDigits = new Map<string, [string, string]>(
  PAIRS.map((pair) => [digits(pair[0]), pair]),
);

function asRecord(value: unknown): RecordValue {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as RecordValue)
    : {};
}

function asItems(value: unknown): unknown[] {
  if (value == null) return [];
  return Array.isArray(value) ? value : [value];
}

function text(value: unknown, fallback = ""): string {
  return value == null ? fallback : String(value);
}

function numberOrNull(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function dateKey(value: unknown): string {
  const source = text(value).trim();
  const iso = source.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  const local = source.match(/^(\d{1,2})[./-](\d{1,2})[./-](\d{4})/);
  return local
    ? `${local[3]}-${local[2].padStart(2, "0")}-${local[1].padStart(2, "0")}`
    : "";
}

function timeValue(value: unknown): number {
  const parsed = Date.parse(text(value));
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function emptyCounts(): Counts {
  return {
    aircraft: 0,
    tests: 0,
    flights: 0,
    balanceReadings: 0,
    trackReadings: 0,
    fftCaptures: 0,
    fftSamples: 0,
  };
}

function complianceLimit(axis: string, part: string, ended: unknown): number | null {
  const date = dateKey(ended);
  if (part === "Haupt" && axis === "Vertikal") return 0.2;
  if (part === "Haupt" && axis === "Lateral")
    return date && date >= "2023-07-20" ? 0.25 : 0.3;
  if (part === "Heck" && (axis === "Radial" || axis === "Axial")) return 0.2;
  return null;
}

function recordFinding(
  findings: Map<string, number>,
  code: string,
  amount = 1,
): void {
  findings.set(code, (findings.get(code) ?? 0) + amount);
}

function buildFftAudit(
  aircraft: RecordValue,
  counters: { captures: number; samples: number; findings: Map<string, number> },
  counts: Counts,
): void {
  for (const testValue of asItems(aircraft.Tests)) {
    for (const flightValue of asItems(asRecord(testValue).Flights)) {
      for (const captureValue of asItems(asRecord(flightValue).FftData)) {
        const capture = asRecord(captureValue);
        counters.captures += 1;
        counts.fftCaptures += 1;
        const label = text(capture.FftRange);
        const rangeMatch = label.match(/0\s*-\s*(\d+)\s*[TR]?\s*Harmonic/i);
        if (label && !rangeMatch) recordFinding(counters.findings, "unknown-range-label");

        for (const transducerValue of asItems(capture.Transducers)) {
          const samples = asItems(asRecord(transducerValue).Samples);
          const seenFrequency = new Set<number>();
          let previousFrequency = Number.NEGATIVE_INFINITY;
          for (const sampleValue of samples) {
            const sample = asRecord(sampleValue);
            counters.samples += 1;
            counts.fftSamples += 1;
            const frequency = numberOrNull(sample.Frequency);
            const amplitude = numberOrNull(sample.Amplitude);
            if (frequency == null || amplitude == null) {
              recordFinding(counters.findings, "invalid-sample-number");
            } else {
              if (amplitude < 0) recordFinding(counters.findings, "negative-amplitude");
              if (seenFrequency.has(frequency))
                recordFinding(counters.findings, "repeated-frequency");
              if (frequency < previousFrequency)
                recordFinding(counters.findings, "frequency-not-increasing");
              seenFrequency.add(frequency);
              previousFrequency = frequency;
            }
          }
        }
      }
    }
  }
}

function finishFftAudit(
  counters: { captures: number; samples: number; findings: Map<string, number> },
): FftAudit {
  const labels: Record<string, string> = {
    "invalid-sample-number": "Samples missing a finite frequency or amplitude",
    "negative-amplitude": "Negative FFT amplitudes",
    "repeated-frequency": "Repeated frequency bins within a transducer capture",
    "frequency-not-increasing": "Frequency bins that are not in ascending order",
    "unknown-range-label": "FFT range labels that do not match the documented harmonic format",
  };
  return {
    captures: counters.captures,
    samples: counters.samples,
    findings: [...counters.findings.entries()].map(([code, count]) => ({
      code,
      label: labels[code] ?? code,
      count,
      severity: code === "invalid-sample-number" ? "critical" : "warning",
    })),
    preserved: true,
  };
}

function makeAnalytics(
  counts: Counts,
  conditionCounts: Map<string, number>,
  partCounts: Map<string, number>,
  violations: Map<string, { limit: number; count: number }>,
  histograms: HistogramData,
): Analytics {
  return {
    counts,
    byCondition: [...conditionCounts.entries()]
      .map(([name, flights]) => ({ name, flights }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    byPart: [...partCounts.entries()]
      .map(([name, readings]) => ({ name, readings }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    compliance: [...violations.entries()].map(([axis, value]) => ({
      axis,
      limit: value.limit,
      violations: value.count,
    })),
    histograms,
    averageFlightsPerTest:
      counts.tests > 0 ? counts.flights / counts.tests : 0,
  };
}
  return {
    counts,
    byCondition: [...conditionCounts.entries()]
      .map(([name, flights]) => ({ name, flights }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    byPart: [...partCounts.entries()]
      .map(([name, readings]) => ({ name, readings }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    compliance: [...violations.entries()].map(([axis, value]) => ({
      axis,
      limit: value.limit,
      violations: value.count,
    })),
  };
}


async function jsonRootIsArray(path: string): Promise<boolean> {
  const source = createReadStream(path, {
    encoding: "utf8",
    highWaterMark: 4096,
  });

  try {
    for await (const chunk of source) {
      const first = String(chunk).match(/\S/);
      if (first) return first[0] === "[";
    }
  } finally {
    source.destroy();
  }

  throw new Error("The uploaded file is empty or does not contain JSON data.");
}

async function* aircraftAtPath(
  path: string,
  filter?: string,
): AsyncGenerator<RecordValue> {
  const source = createReadStream(path);
  const stages = [source, parser()];

  const stream = chain(
    filter
      ? [...stages, pick({ filter }), streamArray()]
      : [...stages, streamArray()],
  );

  for await (const item of stream) {
    const value = asRecord((item as { value?: unknown }).value);
    if (Object.keys(value).length) yield value;
  }
}

async function* aircraftItems(
  path: string,
): AsyncGenerator<RecordValue> {
  // Format 1: a JSON root array of aircraft records.
  if (await jsonRootIsArray(path)) {
    yield* aircraftAtPath(path);
    return;
  }

  // Format 2: an Aircraft array at the root or inside a common wrapper.
  const candidates = [
    "Aircraft",
    "aircraft",
    "aircrafts",
    "data.Aircraft",
    "data.aircraft",
    "dataset.Aircraft",
    "Dataset.Aircraft",
    "Export.Aircraft",
  ];

  for (const candidate of candidates) {
    let found = false;

    for await (const aircraft of aircraftAtPath(path, candidate)) {
      found = true;
      yield aircraft;
    }

    if (found) return;
  }

  throw new Error(
    'Could not find aircraft records. Expected an "Aircraft" array, ' +
      '"data.Aircraft", "aircraft", or a root array of aircraft records.',
  );
}


async function collectMetadata(path: string): Promise<RecordValue> {
  // A root array has no separate metadata object.
  if (await jsonRootIsArray(path)) {
    return {};
  }

  const source = createReadStream(path);

  const stream = chain([
    source,
    parser(),
    ignore({ filter: "Aircraft" }),
    streamObject(),
  ]);

  const metadata: RecordValue = {};

  for await (const entry of stream) {
    const { key, value } = entry as {
      key: string;
      value: unknown;
    };

    metadata[key] = value;
  }

  return metadata;
}

async function scanDataset(path: string): Promise<{
  counts: Counts;
  flights: FlightReading[];
  analytics: Analytics;
  fftAudit: FftAudit;
}> {
  const counts = emptyCounts();
  const flights: FlightReading[] = [];
  const conditionCounts = new Map<string, number>();
  const partCounts = new Map<string, number>();
  const violations = new Map<string, { limit: number; count: number }>();
  const histograms: HistogramData = {
    allFlightsPerTest: [],
    hauptFlightsPerTest: [],
    heckFlightsPerTest: [],
    hauptBodenlaufPerTest: [],
    hauptOtherConditionsPerTest: [],
    heckBodenlaufPerTest: [],
    heckOtherConditionsPerTest: [],
  };
  const fft = {
    captures: 0,
    samples: 0,
    findings: new Map<string, number>(),
  };

  for await (const aircraft of aircraftItems(path)) {
    counts.aircraft += 1;
    buildFftAudit(aircraft, fft, counts);
    const tailNumber = text(aircraft.TailNumber, "Unknown tail");
    for (const testValue of asItems(aircraft.Tests)) {
      const test = asRecord(testValue);
      counts.tests += 1;
      const testNumber = text(test.TestNumber, "—");

      let allFlights = 0;
      let hauptFlights = 0;
      let heckFlights = 0;
      let hauptBodenlauf = 0;
      let hauptOtherConditions = 0;
      let heckBodenlauf = 0;
      let heckOtherConditions = 0;

      for (const flightValue of asItems(test.Flights)) {
        const flight = asRecord(flightValue);
        counts.flights += 1;
        const balanceData = asItems(flight.BalanceData).map((entry) => {
          const item = asRecord(entry);
          const reading: BalanceReading = {
            testCondition: text(item.TestCondition, "Unknown"),
            part: text(item.Part, "Unknown"),
            axis: text(item.Axis, "Unknown"),
            amplitude: numberOrNull(item.Amplitude),
            phase: numberOrNull(item.Phase),
          };
          counts.balanceReadings += 1;
          const partKey = reading.part;
          partCounts.set(partKey, (partCounts.get(partKey) ?? 0) + 1);
          const limit = complianceLimit(reading.axis, reading.part, flight.Ended);
          if (limit != null && reading.amplitude != null) {
            const previous = violations.get(reading.axis) ?? { limit, count: 0 };
            if (Math.abs(reading.amplitude) > limit) previous.count += 1;
            violations.set(reading.axis, previous);
          }
          return reading;
        });
        const trackData = asItems(flight.TrackData).map((entry) => {
          const item = asRecord(entry);
          counts.trackReadings += 1;
          partCounts.set(
            text(item.Part, "Unknown"),
            (partCounts.get(text(item.Part, "Unknown")) ?? 0) + 1,
          );
          return {
            TestCondition: item.TestCondition,
            Part: item.Part,
            TrackSplit: item.TrackSplit,
            BladeHeights: item.BladeHeights,
          };
        });
        const conditions = new Set<string>();
        for (const reading of balanceData) conditions.add(reading.testCondition);
        for (const reading of trackData)
          conditions.add(text(reading.TestCondition, "Unknown"));
        for (const condition of conditions)
          conditionCounts.set(condition, (conditionCounts.get(condition) ?? 0) + 1);
        // Histogram counts: every flight counts, even without measurement data.
        allFlights += 1;

        // Haupt is identified in either BalanceData or TrackData.
        const hasHaupt =
          balanceData.some((reading) => reading.part === "Haupt") ||
          trackData.some((reading) => text(reading.Part) === "Haupt");

        // Heck is identified in BalanceData only.
        const hasHeck = balanceData.some(
          (reading) => reading.part === "Heck",
        );

        // Bodenlauf-only means exactly one unique condition: Bodenlauf.
        const isBodenlaufOnly =
          conditions.size === 1 && conditions.has("Bodenlauf");

        // Other conditions can appear alongside other conditions.
        const otherConditions = [
          "Hovern OGE",
          "90 Knoten",
          "110 Knoten",
          "130 Knoten",
        ];
        const hasOtherCondition = otherConditions.some((condition) =>
          conditions.has(condition),
        );

        if (hasHaupt) {
          hauptFlights += 1;

          if (isBodenlaufOnly) {
            hauptBodenlauf += 1;
          }

          if (hasOtherCondition) {
            hauptOtherConditions += 1;
          }
        }

        if (hasHeck) {
          heckFlights += 1;

          if (isBodenlaufOnly) {
            heckBodenlauf += 1;
          }

          if (hasOtherCondition) {
            heckOtherConditions += 1;
          }
        }
        if (flights.length < MAX_FLIGHT_ROWS) {
          flights.push({
            tailNumber,
            testNumber,
            flightNumber: text(flight.FlightNumber, "—"),
            started: flight.Started == null ? null : text(flight.Started),
            ended: flight.Ended == null ? null : text(flight.Ended),
            balanceData,
            trackData,
          });
        }
      }

      // Store one histogram value for this test.
      histograms.allFlightsPerTest.push(allFlights);
      histograms.hauptFlightsPerTest.push(hauptFlights);
      histograms.heckFlightsPerTest.push(heckFlights);
      histograms.hauptBodenlaufPerTest.push(hauptBodenlauf);
      histograms.hauptOtherConditionsPerTest.push(hauptOtherConditions);
      histograms.heckBodenlaufPerTest.push(heckBodenlauf);
      histograms.heckOtherConditionsPerTest.push(heckOtherConditions);
    }
  }

  // A separate token pass counts FFT containers as captures, including captures
  // with no transducer samples. buildFftAudit already counts these per aircraft.
  return {
    counts,
    flights,
    analytics: makeAnalytics(
      counts,
      conditionCounts,
      partCounts,
      violations,
      histograms,
    ),
    fftAudit: finishFftAudit(fft),
  };
}

function sessionResponse(session: Session): RecordValue {
  return {
    sessionId: session.id,
    filename: session.filename,
    bytes: session.bytes,
    importedAt: session.importedAt,
    expiresInSeconds: Math.max(0, Math.ceil((session.expiresAt - Date.now()) / 1000)),
    cleaned: Boolean(session.cleanedPath),
    counts: session.counts,
    log: session.log,
  };
}

function touch(session: Session): void {
  session.expiresAt = Date.now() + SESSION_TTL_MS;
}

async function removeSession(session: Session): Promise<void> {
  sessions.delete(session.id);
  await rm(session.directory, { recursive: true, force: true });
}

const expiryTimer = setInterval(() => {
  const now = Date.now();
  for (const session of sessions.values()) {
    if (session.expiresAt <= now) void removeSession(session);
  }
}, 60_000);
expiryTimer.unref();

export async function createDataSession(
  request: NodeJS.ReadableStream,
  filename: string,
  declaredLength?: number,
): Promise<RecordValue> {
  if (declaredLength != null && declaredLength > MAX_UPLOAD_BYTES) {
    throw new Error("The upload is larger than the 2 GB session limit.");
  }
  const id = randomUUID();
  const directory = await mkdtemp(join(tmpdir(), `ch53-session-${id}-`));
  const rawPath = join(directory, "source.json");
  let bytes = 0;
  const output = createWriteStream(rawPath, { flags: "wx" });
  try {
    for await (const chunk of request) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
      bytes += buffer.length;
      if (bytes > MAX_UPLOAD_BYTES) {
        output.destroy(new Error("The upload is larger than the 2 GB session limit."));
        throw new Error("The upload is larger than the 2 GB session limit.");
      }
      if (!output.write(buffer)) await once(output, "drain");
    }
    output.end();
    await once(output, "finish");
    if (bytes === 0) throw new Error("The uploaded file is empty.");
    const scanned = await scanDataset(rawPath);
    if (scanned.counts.aircraft === 0)
      throw new Error('No "Aircraft" array was found in the uploaded JSON.');
    const session: Session = {
      id,
      directory,
      rawPath,
      filename: filename.trim() || "aircraft-data.json",
      bytes,
      importedAt: new Date().toISOString(),
      expiresAt: Date.now() + SESSION_TTL_MS,
      counts: scanned.counts,
      flights: scanned.flights,
      analytics: scanned.analytics,
      fftAudit: scanned.fftAudit,
      log: [
        `Imported ${filename || "aircraft-data.json"} (${(bytes / (1024 ** 3)).toFixed(2)} GB).`,
        `Read ${scanned.counts.aircraft} aircraft, ${scanned.counts.tests} tests, and ${scanned.counts.flights} flights.`,
        `FFT audit found ${scanned.fftAudit.findings.reduce((sum, item) => sum + item.count, 0)} data-quality flags; no FFT samples were changed.`,
      ],
      separatedTests: [],
    };
    sessions.set(id, session);
    return sessionResponse(session);
  } catch (error) {
    output.destroy();
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

export function getDataSession(id: string): RecordValue | null {
  const session = sessions.get(id);
  if (!session || session.expiresAt <= Date.now()) return null;
  touch(session);
  return sessionResponse(session);
}

export function getSessionAnalytics(id: string): Analytics | null {
  const session = sessions.get(id);
  if (!session || session.expiresAt <= Date.now()) return null;
  touch(session);
  return session.analytics;
}

export function getFftAudit(id: string): FftAudit | null {
  const session = sessions.get(id);
  if (!session || session.expiresAt <= Date.now()) return null;
  touch(session);
  return session.fftAudit;
}

export function getSessionFlights(
  id: string,
  filters: { search?: string; tailNumber?: string; condition?: string; page: number; limit: number },
): { items: FlightReading[]; page: number; limit: number; total: number } | null {
  const session = sessions.get(id);
  if (!session || session.expiresAt <= Date.now()) return null;
  touch(session);
  const needle = (filters.search ?? "").toLocaleLowerCase();
  const matched = session.flights.filter((flight) => {
    const matchesTail = !filters.tailNumber || flight.tailNumber === filters.tailNumber;
    const matchesCondition =
      !filters.condition ||
      flight.balanceData.some((reading) => reading.testCondition === filters.condition) ||
      flight.trackData.some((reading) => reading.TestCondition === filters.condition);
    const matchesSearch =
      !needle ||
      `${flight.tailNumber} ${flight.testNumber} ${flight.flightNumber} ${flight.started ?? ""}`
        .toLocaleLowerCase()
        .includes(needle);
    return matchesTail && matchesCondition && matchesSearch;
  });
  const start = (filters.page - 1) * filters.limit;
  return {
    items: matched.slice(start, start + filters.limit),
    page: filters.page,
    limit: filters.limit,
    total: matched.length,
  };
}

type OutputRecord = { path: string } | { group: string };

function testFirstDate(test: RecordValue): number {
  const values = asItems(test.Flights)
    .map((flight) => timeValue(asRecord(flight).Started))
    .filter(Number.isFinite);
  return values.length ? Math.min(...values) : timeValue(test.Started);
}

function recomputeTestRange(test: RecordValue): void {
  const flights = asItems(test.Flights).map(asRecord);
  const starts = flights.map((flight) => timeValue(flight.Started)).filter(Number.isFinite);
  const ends = flights.map((flight) => timeValue(flight.Ended)).filter(Number.isFinite);
  if (starts.length) test.Started = new Date(Math.min(...starts)).toISOString();
  if (ends.length) test.Ended = new Date(Math.max(...ends)).toISOString();
}

const DAY_MS = 24 * 60 * 60 * 1000;

function dayNumber(value: unknown): number {
  const key = dateKey(value);
  if (!key) return Number.NaN;
  return Date.UTC(Number(key.slice(0, 4)), Number(key.slice(5, 7)) - 1, Number(key.slice(8, 10))) / DAY_MS;
}

// First and last flight day of a test (calendar days), or null if no flight has a usable date.
function testDayRange(test: RecordValue): { first: number; last: number } | null {
  const flights = asItems(test.Flights).map(asRecord);
  const firsts = flights.map((flight) => dayNumber(flight.Started)).filter(Number.isFinite);
  const lasts = flights
    .map((flight) => {
      const ended = dayNumber(flight.Ended);
      return Number.isFinite(ended) ? ended : dayNumber(flight.Started);
    })
    .filter(Number.isFinite);
  if (!firsts.length || !lasts.length) return null;
  return { first: Math.min(...firsts), last: Math.max(...lasts) };
}

// Merges tests of one aircraft when the gap between the last flight of one test and the
// first flight of the next test is at most `gapDays` calendar days. Chains merge step by step.
function mergeTestsByGap(
  tests: RecordValue[],
  gapDays: number,
  tail: string,
  log: string[],
): RecordValue[] {
  const limit = Math.max(1, Math.floor(gapDays));
  const dated = tests
    .map((test, index) => ({ test, index, range: testDayRange(test) }))
    .filter((item): item is { test: RecordValue; index: number; range: { first: number; last: number } } => item.range !== null)
    .sort((a, b) => a.range.first - b.range.first);

  const absorbed = new Set<number>();
  let current: (typeof dated)[number] | null = null;
  let currentLast = 0;
  for (const item of dated) {
    if (current && item.range.first - currentLast <= limit) {
      const target = current.test;
      const gap = item.range.first - currentLast;
      log.push(
        `${tail}: merged test ${text(item.test.TestNumber, "?")} into test ${text(target.TestNumber, "?")} (${gap} day${gap === 1 ? "" : "s"} between last and first flight).`,
      );
      target.Flights = [...asItems(target.Flights), ...asItems(item.test.Flights)];
      const targetEnd = timeValue(target.Ended);
      const itemEnd = timeValue(item.test.Ended);
      if (Number.isFinite(itemEnd) && !(Number.isFinite(targetEnd) && targetEnd >= itemEnd)) {
        target.Ended = item.test.Ended;
      }
      absorbed.add(item.index);
      currentLast = Math.max(currentLast, item.range.last);
    } else {
      current = item;
      currentLast = item.range.last;
    }
  }
  return tests.filter((_, index) => !absorbed.has(index));
}

// Splits a test at every gap where the calendar-day difference between
// the previous flight's Ended date and the next flight's Started date
// is greater than or equal to gapDays.
//
// Flight records are preserved, including BalanceData, TrackData and FftData.
function splitTestsByGap(
  tests: RecordValue[],
  gapDays: number,
  tail: string,
  log: string[],
): RecordValue[] {
  const limit = Math.max(1, Math.floor(gapDays));

  // Track existing test numbers so new test numbers remain unique.
  const usedNumbers = new Set(
    tests
      .map((test) => text(test.TestNumber).trim())
      .filter(Boolean),
  );

  const numericNumbers = tests
    .map((test) => text(test.TestNumber).trim())
    .filter((value) => value !== '')
    .map(Number)
    .filter(
      (value) => Number.isInteger(value) && value >= 0,
    );

  let nextNumericNumber = numericNumbers.length
    ? Math.max(...numericNumbers) + 1
    : 1;

  let generatedSuffix = 1;

  const makeUniqueTestNumber = (
    originalValue: unknown,
  ): string | number => {
    const original = text(originalValue).trim();
    const wasNumber = typeof originalValue === 'number';

    const numericOriginal =
      original !== '' &&
      Number.isInteger(Number(original)) &&
      Number(original) >= 0;

    if (numericOriginal || original === '') {
      while (usedNumbers.has(String(nextNumericNumber))) {
        nextNumericNumber += 1;
      }

      const result = nextNumericNumber++;
      usedNumbers.add(String(result));

      return wasNumber ? result : String(result);
    }

    let candidate = '';

    do {
      candidate = `${original || 'test'}-split-${generatedSuffix++}`;
    } while (usedNumbers.has(candidate));

    usedNumbers.add(candidate);

    return candidate;
  };

  const result: RecordValue[] = [];

  for (const test of tests) {
    const flights = asItems(test.Flights);

    // A test with fewer than two flights cannot be split.
    if (flights.length < 2) {
      result.push(test);
      continue;
    }

    const chunks: unknown[][] = [];
    let currentChunk: unknown[] = [];
    let previousFlight: RecordValue | null = null;

    for (const flightValue of flights) {
      const flight = asRecord(flightValue);

      if (previousFlight) {
        // Prefer the previous flight's end date.
        // If it is missing, fall back to its start date.
        const endedDay = dayNumber(previousFlight.Ended);

        const previousEnd = Number.isFinite(endedDay)
          ? endedDay
          : dayNumber(previousFlight.Started);

        const nextStart = dayNumber(flight.Started);

        const gap = nextStart - previousEnd;

        // The threshold is inclusive: gap >= limit triggers a split.
        if (Number.isFinite(gap) && gap >= limit) {
          chunks.push(currentChunk);
          currentChunk = [];

          log.push(
            `${tail}: split test ${text(test.TestNumber, '?')} ` +
            `before flight ${text(flight.FlightNumber, '—')} ` +
            `(${gap} calendar days from the previous flight's end; ` +
            `threshold ${limit}).`,
          );
        }
      }

      // Keep the original flight object and all its nested data.
      currentChunk.push(flightValue);
      previousFlight = flight;
    }

    if (currentChunk.length) {
      chunks.push(currentChunk);
    }

    // If no gap qualified, preserve the original test.
    if (chunks.length === 1) {
      result.push(test);
      continue;
    }

    chunks.forEach((chunk, index) => {
      // Copy the test metadata and replace only its Flights array.
      const splitTest: RecordValue = {
        ...test,
        Flights: chunk,
      };

      // The first part keeps the original number.
      // Additional parts receive unique test numbers.
      if (index > 0) {
        splitTest.TestNumber = makeUniqueTestNumber(
          test.TestNumber,
        );

        log.push(
          `${tail}: created test ${text(splitTest.TestNumber, '?')} ` +
          `from test ${text(test.TestNumber, '?')} ` +
          `with ${chunk.length} flight(s).`,
        );
      }

      // Recalculate the date range for each resulting test.
      recomputeTestRange(splitTest);

      result.push(splitTest);
    });
  }

  return result;
}

const SMALL_AMP_THRESHOLD = 0.0005;

function normKey(value: unknown): string {
  return String(value ?? "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

// Removes a flight's near-zero balance measurements (|Amplitude| <= threshold) and every
// reading linked to the same Part + TestCondition: TrackData, FftData, and the matching
// predicted values and adjustments inside SolveResults. Returns the removed part names.
function removeSmallAmplitudeFromFlight(flight: RecordValue, threshold: number): string[] {
  const triggers = new Set<string>();
  const removedParts = new Set<string>();
  const keptBalance: RecordValue[] = [];
  for (const entry of asItems(flight.BalanceData).map(asRecord)) {
    const amplitude = numberOrNull(entry.Amplitude);
    if (amplitude != null && Math.abs(amplitude) <= threshold) {
      triggers.add(`${normKey(entry.Part)}|${normKey(entry.TestCondition)}`);
      removedParts.add(text(entry.Part));
    } else {
      keptBalance.push(entry);
    }
  }
  if (!triggers.size) return [];
  flight.BalanceData = keptBalance;
  const matches = (item: RecordValue) => triggers.has(`${normKey(item.Part)}|${normKey(item.TestCondition)}`);
  flight.TrackData = asItems(flight.TrackData).map(asRecord).filter((item) => !matches(item));
  flight.FftData = asItems(flight.FftData).map(asRecord).filter((item) => !matches(item));
  const triggerParts = new Set([...triggers].map((key) => key.split("|")[0]));
  flight.SolveResults = asItems(flight.SolveResults).map(asRecord).map((solve) => {
    solve.PredictedBalance = asItems(solve.PredictedBalance).map(asRecord).filter((item) => !matches(item));
    solve.PredictedTrack = asItems(solve.PredictedTrack).map(asRecord).filter((item) => !matches(item));
    solve.Adjustments = asItems(solve.Adjustments).map(asRecord).map((adjustment) => {
      adjustment.Parts = asItems(adjustment.Parts)
        .map(asRecord)
        .filter((item) => !triggerParts.has(normKey(item.Part)));
      return adjustment;
    });
    return solve;
  });
  return [...removedParts].sort();
}

// Qa (track & balance quality) setup. CH-53 and H145/BK117 use their documented limits.
// Any other aircraft type falls back to a structure read from that aircraft's own data
// (whichever test conditions and part/axis combinations actually appear) with generic
// default limits, so Qa can still be computed, approximately, for unlisted aircraft types.
const QA_DEFAULT_FACTOR = 10;
const QA_CH53_CONDS = ["Bodenlauf", "Hovern OGE", "90 Knoten", "110 Knoten", "130 Knoten"];
const QA_CH53_COLS: Array<[string, string]> = [["Haupt", "Lateral"], ["Haupt", "Vertikal"], ["Heck", "Radial"], ["Heck", "Axial"]];
const QA_H145_CONDS = ["Ground", "Hover (IGE)", "80 KIAS", "120 KIAS", "VH"];
const QA_H145_COLS: Array<[string, string]> = [["Main", "Lateral"], ["Main", "Vertical"], ["Tail", "Radial"], ["Cabin", "Vert Cabi"]];
const QA_LATERAL_CHANGE_DAY = Date.UTC(2023, 6, 20) / DAY_MS;

type QaFamily = "CH53" | "H145" | "GENERIC";
type QaConfig = {
  family: QaFamily;
  conds: string[];
  cols: Array<[string, string]>;
  rotors: string[];
  trackPart: string;
  trackLimit: number;
};

function qaFamily(aircraftType: unknown): QaFamily {
  const key = normKey(aircraftType);
  if (key.includes("ch53")) return "CH53";
  if (key.includes("h145") || key.includes("bk117")) return "H145";
  return "GENERIC";
}

// For an unlisted aircraft type: read which test conditions and Part/Axis balance
// combinations actually appear in this aircraft's own tests, and use the most common
// TrackData Part as the track rotor. Generic limits (0.2 balance, ~2 in / 50.8 mm track)
// are then applied uniformly; this is an approximation, not a documented limit table.
function buildQaConfig(aircraftType: unknown, trackUnits: unknown, tests: RecordValue[]): QaConfig {
  const family = qaFamily(aircraftType);
  const mm = !trackUnits || normKey(trackUnits).includes("mm");
  if (family === "CH53") {
    return { family, conds: QA_CH53_CONDS, cols: QA_CH53_COLS, rotors: ["Haupt", "Heck"], trackPart: "Haupt", trackLimit: mm ? 50.8 : 2.0 };
  }
  if (family === "H145") {
    return { family, conds: QA_H145_CONDS, cols: QA_H145_COLS, rotors: ["Main", "Tail", "Cabin"], trackPart: "Main", trackLimit: mm ? 40.0 : 40.0 / 25.4 };
  }
  const conds: string[] = [];
  const cols: Array<[string, string]> = [];
  const colKeys = new Set<string>();
  const trackPartCounts = new Map<string, number>();
  const addCond = (value: unknown) => {
    const name = text(value);
    if (name && !conds.some((cond) => normKey(cond) === normKey(name))) conds.push(name);
  };
  for (const test of tests) {
    for (const flight of asItems(test.Flights).map(asRecord)) {
      for (const entry of asItems(flight.BalanceData).map(asRecord)) {
        addCond(entry.TestCondition);
        const part = text(entry.Part);
        const axis = text(entry.Axis);
        const key = `${normKey(part)}|${normKey(axis)}`;
        if (part && axis && !colKeys.has(key)) {
          colKeys.add(key);
          cols.push([part, axis]);
        }
      }
      for (const entry of asItems(flight.TrackData).map(asRecord)) {
        addCond(entry.TestCondition);
        const part = text(entry.Part);
        if (part) trackPartCounts.set(part, (trackPartCounts.get(part) ?? 0) + 1);
      }
    }
  }
  const trackPart = [...trackPartCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? "";
  const rotors: string[] = [];
  for (const [rotor] of cols) if (!rotors.some((item) => normKey(item) === normKey(rotor))) rotors.push(rotor);
  return { family: "GENERIC", conds, cols, rotors, trackPart, trackLimit: mm ? 50.8 : 2.0 };
}

function qaConditionKey(cfg: QaConfig, cond: unknown): string {
  const key = normKey(cond);
  if (key === "parallel") {
    if (cfg.family === "H145") return normKey("Ground");
    if (cfg.family === "CH53") return normKey("Bodenlauf");
  }
  return key;
}

function qaBalanceLimit(cfg: QaConfig, rotor: string, axis: string, cond: string, flightDay: number): number {
  if (cfg.family === "CH53") {
    if (normKey(axis) === "lateral") return Number.isFinite(flightDay) && flightDay < QA_LATERAL_CHANGE_DAY ? 0.3 : 0.25;
    return 0.2;
  }
  if (cfg.family === "H145") {
    const base: Record<string, number> = { ground: 0.4, hoverige: 0.1, "80kias": 0.15, "120kias": 0.15, vh: 0.15 };
    if (normKey(rotor) === "tail" && normKey(cond) === "ground") return 0.2;
    return base[normKey(cond)] ?? 0.2;
  }
  return 0.2;
}

// Qa per rotor part for one flight: the average, over every (condition x part/axis) term,
// of value / limit, with a missing measurement counted as 10x its limit. Returns null when
// this aircraft's Qa structure could not be determined (no usable conditions or columns).
function computeFlightRotorQa(flight: RecordValue, cfg: QaConfig, flightDay: number): Map<string, number> | null {
  if (!cfg.conds.length || !cfg.cols.length) return null;
  const track = new Map<string, number>();
  for (const entry of asItems(flight.TrackData).map(asRecord)) {
    if (normKey(entry.Part) === normKey(cfg.trackPart)) {
      const value = numberOrNull(entry.TrackSplit);
      if (value != null) track.set(qaConditionKey(cfg, entry.TestCondition), Math.abs(value));
    }
  }
  const balance = new Map<string, number>();
  for (const entry of asItems(flight.BalanceData).map(asRecord)) {
    const value = numberOrNull(entry.Amplitude);
    if (value != null) {
      balance.set(`${qaConditionKey(cfg, entry.TestCondition)}|${normKey(entry.Part)}|${normKey(entry.Axis)}`, Math.abs(value));
    }
  }
  const terms: Array<{ rotor: string; ratio: number }> = [];
  for (const cond of cfg.conds) {
    const key = qaConditionKey(cfg, cond);
    const trackValue = track.get(key);
    terms.push({ rotor: cfg.trackPart, ratio: trackValue == null ? QA_DEFAULT_FACTOR : trackValue / cfg.trackLimit });
    for (const [rotor, axis] of cfg.cols) {
      const value = balance.get(`${key}|${normKey(rotor)}|${normKey(axis)}`);
      const limit = qaBalanceLimit(cfg, rotor, axis, cond, flightDay);
      terms.push({ rotor, ratio: value == null ? QA_DEFAULT_FACTOR : value / limit });
    }
  }
  const rotorQa = new Map<string, number>();
  for (const rotor of cfg.rotors) {
    const ratios = terms.filter((term) => normKey(term.rotor) === normKey(rotor)).map((term) => term.ratio);
    if (ratios.length) rotorQa.set(rotor, ratios.reduce((sum, value) => sum + value, 0) / ratios.length);
  }
  return rotorQa;
}

// Deletes an entire test when any rotor's Qa gets worse from one flight to the next flight
// within that same test (comparing flights in their stored order, not sorted by date).
function removeTestsWithWorseningQa(
  tests: RecordValue[],
  cfg: QaConfig,
  tail: string,
  log: string[],
): RecordValue[] {
  return tests.filter((test) => {
    let previous: Map<string, number> | null = null;
    let reason = "";
    for (const flight of asItems(test.Flights).map(asRecord)) {
      const current = computeFlightRotorQa(flight, cfg, dayNumber(flight.Started));
      if (previous && current) {
        for (const [rotor, newQa] of current) {
          const oldQa = previous.get(rotor);
          if (oldQa != null && newQa > oldQa && !reason) {
            reason = `${rotor} Qa increased (${oldQa.toFixed(4)} -> ${newQa.toFixed(4)})`;
          }
        }
      }
      if (current) previous = current;
    }
    if (reason) {
      log.push(`${tail}: removed test ${text(test.TestNumber, "?")} (${reason}).`);
      return false;
    }
    return true;
  });
}

function applyIntraAircraftRules(
  aircraft: RecordValue,
  options: CleanupOptions,
  log: string[],
  separatedTests: unknown[],
  aircraftType: unknown,
  trackUnits: unknown,
): RecordValue {
  const tail = text(aircraft.TailNumber);
  if (options.removeEmptyFlights) {
    for (const testValue of asItems(aircraft.Tests)) {
      const test = asRecord(testValue);
      const flights = asItems(test.Flights);
      const kept = flights.filter((flightValue) => {
        const flight = asRecord(flightValue);
        const hasBalance = asItems(flight.BalanceData).length > 0;
        const hasTrack = asItems(flight.TrackData).length > 0;
        const hasFft = asItems(flight.FftData).length > 0;
        if (!hasBalance && !hasTrack && !hasFft) {
          log.push(`${tail}: removed empty flight ${text(flight.FlightNumber, "—")}.`);
          return false;
        }
        return true;
      });
      test.Flights = kept;
      if (kept.length) recomputeTestRange(test);
    }
  }

  let tests = asItems(aircraft.Tests).map(asRecord);
  if (options.applyDateCorrections && tail === "84+24") {
    const donorIndex = tests.findIndex((test) => dateKey(test.Started) === "2021-04-20");
    const targetIndex = tests.findIndex((test) => dateKey(test.Started) === "2021-03-03");
    if (donorIndex >= 0 && targetIndex >= 0) {
      const donor = tests[donorIndex];
      const target = tests[targetIndex];
      const donorFlights = asItems(donor.Flights);
      const targetFlights = asItems(target.Flights);
      target.Flights = [
        ...targetFlights.slice(0, 1),
        ...donorFlights,
        ...targetFlights.slice(1),
      ];
      recomputeTestRange(target);
      tests = tests.filter((_, index) => index !== donorIndex);
      log.push("84+24: moved the 20.04.2021 single-flight test into the March–May 2021 test.");
    } else {
      log.push("84+24: date correction needs manual review; expected source or target test was not unique.");
    }
  }
  if (options.applyDateCorrections && tail === "84+34") {
    const donorIndex = tests.findIndex((test) => dateKey(test.Started) === "2020-08-31" && dateKey(test.Ended) === "2020-08-31");
    const targetIndex = tests.findIndex((test) => dateKey(test.Started) === "2020-08-31" && dateKey(test.Ended) === "2020-09-03");
    if (donorIndex >= 0 && targetIndex >= 0) {
      const donorFlights = asItems(tests[donorIndex].Flights);
      const targetFlights = asItems(tests[targetIndex].Flights);
      tests[targetIndex].Flights = [
        ...targetFlights.slice(0, 3),
        ...donorFlights,
        ...targetFlights.slice(3),
      ];
      recomputeTestRange(tests[targetIndex]);
      tests = tests.filter((_, index) => index !== donorIndex);
      log.push("84+34: inserted the 31.08.2020 three-flight test after flight 3.");
    } else {
      log.push("84+34: date correction needs manual review; expected source or target test was not unique.");
    }
  }
  if (options.applyDateCorrections && tail === "85+07") {
    const target = tests.find((test) => dateKey(test.Started) === "2022-07-23");
    if (target) {
      const flights = asItems(target.Flights);
      if (flights.length > 1) {
        const candidate = asRecord(flights[1]);
        if (asItems(candidate.FftData).length) {
          log.push("85+07: the second flight contains FFT data and was retained for manual review.");
        } else {
          const removed = flights.splice(1, 1);
          target.Flights = flights;
          recomputeTestRange(target);
          log.push(`85+07: removed the second flight from the 23.07.2022 test (${text(asRecord(removed[0]).FlightNumber, "flight number not set")}).`);
        }
      }
    }
  }

  const targeted = new Map<string, Set<string>>([
    ["84+24", new Set(["2018-12-14"])],
    ["84+44", new Set(["2019-10-30"])],
    ["84+48", new Set(["2021-01-21"])],
    ["85+03", new Set(["2018-01-25"])],
  ]);
  if (options.removeTargetedTests) {
    if (tail === "84+51")
      log.push("84+51: the README calls for two test deletions but does not identify their dates; none were removed.");
    const dates = targeted.get(tail);
    if (dates) {
      const before = tests.length;
      tests = tests.filter((test) => {
        const remove = dates.has(dateKey(test.Started));
        if (!remove) return true;
        const hasFft = asItems(test.Flights).some(
          (flight) => asItems(asRecord(flight).FftData).length > 0,
        );
        if (hasFft) {
          log.push(`${tail}: retained targeted test starting ${dateKey(test.Started)} because it contains FFT data.`);
          return true;
        }
        log.push(`${tail}: removed targeted test starting ${dateKey(test.Started)}.`);
        return false;
      });
      if (before === tests.length && dates.size)
        log.push(`${tail}: no test matched the targeted deletion date(s); left records unchanged.`);
    }
    if (tail === "85+01") {
      const extracted = tests.filter((test) => dateKey(test.Started) === "2023-09-07");
      if (extracted.length) {
        separatedTests.push(...extracted.map((test) => ({ TailNumber: tail, ...test })));
        tests = tests.filter((test) => dateKey(test.Started) !== "2023-09-07");
        log.push(`85+01: moved ${extracted.length} test(s) starting 07.09.2023 to AndreasSeparatedTests.`);
      }
    }
  }

  if (options.applyDateCorrections && tail === "84+64") {
    log.push("84+64: split-last-two-flights correction requires a unique new TestNumber; left it for manual review.");
  }
  if (options.mergeEvidenceBasedChains)
    log.push(`${tail}: evidence-based test-chain merging was not run; review the README's unresolved chains before treating merged history as final.`);
  if (options.mergeByDays) tests = mergeTestsByGap(tests, options.mergeGapDays, tail, log);
  if (options.splitByDays) tests = splitTestsByGap(tests, options.splitGapDays, tail, log);
  if (options.removeSmallAmplitude) {
    const smallAmpParts = new Set<string>();
    for (const test of tests) {
      for (const flight of asItems(test.Flights).map(asRecord)) {
        for (const part of removeSmallAmplitudeFromFlight(flight, SMALL_AMP_THRESHOLD)) smallAmpParts.add(part);
      }
    }
    if (smallAmpParts.size)
      log.push(`${tail}: removed near-zero balance measurements (<= ${SMALL_AMP_THRESHOLD} ips) and linked readings for ${[...smallAmpParts].join(", ")}.`);
  }
  if (options.removeWorseningQa) {
    const qaConfig = buildQaConfig(aircraftType, trackUnits, tests);
    tests = removeTestsWithWorseningQa(tests, qaConfig, tail, log);
  }
  aircraft.Tests = tests;
  return aircraft;
}

const EOL = "\r\n";
// 2-space indent, CRLF, nested to the given depth (matches the original file)
const pretty = (value: unknown, depth: number) =>
  JSON.stringify(value, null, 2).split("\n").join(EOL + "  ".repeat(depth));

async function writeObjectFile(path: string, value: unknown): Promise<void> {
  await writeFile(path, pretty(value, 2), "utf8");
}

async function writeDataset(
  outputPath: string,
  metadata: RecordValue,
  records: OutputRecord[],
  groupFiles: Map<string, string[]>,
  separatedTests: unknown[],
): Promise<void> {
  const output = createWriteStream(outputPath, { flags: "w" });
  const write = async (value: string | Buffer) => {
    if (!output.write(value)) await once(output, "drain");
  };
  const meta = { ...metadata };
  delete meta.Aircraft;
  const currentSeparated = asItems(meta.AndreasSeparatedTests);
  if (separatedTests.length) meta.AndreasSeparatedTests = [...currentSeparated, ...separatedTests];

  await write("{" + EOL);
  for (const [key, value] of Object.entries(meta)) {
    await write(`  ${JSON.stringify(key)}: ${pretty(value, 1)},${EOL}`);
  }
  await write(`  "Aircraft": [${EOL}`);
  let written = 0;
  for (const record of records) {
    if (written) await write("," + EOL);
    await write("    ");
    if ("path" in record) {
      const input = createReadStream(record.path);
      for await (const chunk of input) await write(chunk as Buffer);
    } else {
      const paths = groupFiles.get(record.group) ?? [];
      const aircraftGroup = (
        await Promise.all(paths.map((path) => import("node:fs/promises").then((fs) => fs.readFile(path, "utf8"))))
      ).map((source) => JSON.parse(source) as RecordValue);
      if (aircraftGroup.length) {
        const keeper = aircraftGroup.find((item) => {
          const tail = text(item.TailNumber);
          return pairByDigits.get(digits(tail))?.[0] === tail;
        }) ?? aircraftGroup[0];
        const combinedTests = aircraftGroup.flatMap((item) => asItems(item.Tests).map(asRecord));
        combinedTests.sort((a, b) => testFirstDate(a) - testFirstDate(b));
        keeper.Tests = combinedTests;
        await write(pretty(keeper, 2));
      }
    }
    written += 1;
  }
  await write(`${EOL}  ]${EOL}}`);
  output.end();
  await once(output, "finish");
}

export async function previewCleanup(
  id: string,
  options: CleanupOptions,
): Promise<RecordValue | null> {
  const session = sessions.get(id);
  if (!session || session.expiresAt <= Date.now()) return null;
  touch(session);
  const previewPath = join(session.directory, "preview.json");
  const metadata = await collectMetadata(session.rawPath);
  const records: OutputRecord[] = [];
  const groups = new Map<string, string[]>();
  const groupOrder = new Set<string>();
  const separatedTests: unknown[] = [];
  const operations: string[] = [];
  const warnings: string[] = [];
  let serial = 0;

  for await (const source of aircraftItems(session.rawPath)) {
    const tail = text(source.TailNumber);
    if (options.removeJunkAircraft && (tail === "J" || tail === "V85+00")) {
      const hasFft = asItems(source.Tests).some((test) =>
        asItems(asRecord(test).Flights).some(
          (flight) => asItems(asRecord(flight).FftData).length > 0,
        ),
      );
      if (hasFft) {
        operations.push(`Kept ${tail} because it contains FFT data; manual review required before removing it.`);
        continue;
      }
      operations.push(`Removed non-aircraft entry ${tail}.`);
      continue;
    }
    const aircraft = applyIntraAircraftRules(
      structuredClone(source),
      options,
      operations,
      separatedTests,
      metadata.AircraftType,
      metadata.TrackUnits,
    );
    const canonical = digits(tail);
    const knownPair = pairByDigits.get(canonical);
    const groupKey =
      options.mergeKnownTailVariants && knownPair
        ? canonical
        : options.mergeKnownTailVariants && (tail === "85+01" || tail === "84+97")
          ? `exact-${tail}`
          : "";
    if (groupKey) {
      const group = groups.get(groupKey) ?? [];
      const itemPath = join(session.directory, `aircraft-${serial++}.json`);
      await writeObjectFile(itemPath, aircraft);
      group.push(itemPath);
      groups.set(groupKey, group);
      if (!groupOrder.has(groupKey)) {
        records.push({ group: groupKey });
        groupOrder.add(groupKey);
      }
      if (group.length > 1)
        operations.push(`Combined duplicate tail-number record(s) for ${knownPair?.[0] ?? tail}.`);
    } else {
      const itemPath = join(session.directory, `aircraft-${serial++}.json`);
      await writeObjectFile(itemPath, aircraft);
      records.push({ path: itemPath });
    }
  }
  if (options.mergeEvidenceBasedChains)
    warnings.push("The README describes evidence-based repair-chain merging, but its settings-direction helper has not been validated; chain merges are not performed automatically.");
  if (options.applyDateCorrections)
    warnings.push("The 84+64 split correction was not applied because the README does not define a safe new TestNumber.");
  if (options.removeTargetedTests)
    warnings.push("The two 84+51 test deletions are not specified by date and were left untouched.");

  await writeDataset(previewPath, metadata, records, groups, separatedTests);
  const after = await scanDataset(previewPath);
  const preview = {
    before: session.counts,
    after: after.counts,
    operations,
    warnings,
    fftSamplesChanged: 0,
  };
  session.previewPath = previewPath;
  session.preview = preview;
  session.options = options;
  session.separatedTests = separatedTests;
  session.log.push(`Cleanup preview ready: ${operations.length} proposed actions, ${warnings.length} manual-review warning(s).`);
  session.log.push("All FFT capture and sample values were retained unchanged; only JSON formatting may differ.");
  return preview;
}

export async function applyCleanup(id: string): Promise<RecordValue | null> {
  const session = sessions.get(id);
  if (!session || session.expiresAt <= Date.now()) return null;
  if (!session.previewPath || !session.preview) throw new Error("Create a cleanup preview before applying it.");
  touch(session);
  session.cleanedPath = session.previewPath;
  session.counts = session.preview.after as Counts;
  const scanned = await scanDataset(session.cleanedPath);
  session.flights = scanned.flights;
  session.analytics = scanned.analytics;
  session.fftAudit = scanned.fftAudit;
  session.log.push("Approved cleanup preview applied; source file remains unchanged.");
  return sessionResponse(session);
}

export function getDownload(id: string): { path: string; filename: string; bytes: number } | null {
  const session = sessions.get(id);
  if (!session || session.expiresAt <= Date.now()) return null;
  if (!session.cleanedPath) return null;
  touch(session);
  return {
    path: session.cleanedPath,
    filename: session.filename.replace(/\.json$/i, "") + "_CLEANED.json",
    bytes: 0,
  };
}

export async function closeDataSession(id: string): Promise<boolean> {
  const session = sessions.get(id);
  if (!session) return false;
  await removeSession(session);
  return true;
}
