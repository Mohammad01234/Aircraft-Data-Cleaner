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
type Analytics = {
  counts: Counts;
  byCondition: Array<{ name: string; flights: number }>;
  byPart: Array<{ name: string; readings: number }>;
  compliance: Array<{ axis: string; limit: number; violations: number }>;
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
  };
}

async function* aircraftItems(path: string): AsyncGenerator<RecordValue> {
  const source = createReadStream(path);
  const stream = chain([
    source,
    parser(),
    pick({ filter: "Aircraft" }),
    streamArray(),
  ]);
  for await (const item of stream) {
    const value = asRecord((item as { value?: unknown }).value);
    if (Object.keys(value).length) yield value;
  }
}

async function collectMetadata(path: string): Promise<RecordValue> {
  const source = createReadStream(path);
  const stream = chain([
    source,
    parser(),
    ignore({ filter: "Aircraft" }),
    streamObject(),
  ]);
  const metadata: RecordValue = {};
  for await (const entry of stream) {
    const { key, value } = entry as { key: string; value: unknown };
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
    }
  }

  // A separate token pass counts FFT containers as captures, including captures
  // with no transducer samples. buildFftAudit already counts these per aircraft.
  return {
    counts,
    flights,
    analytics: makeAnalytics(counts, conditionCounts, partCounts, violations),
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

function applyIntraAircraftRules(
  aircraft: RecordValue,
  options: CleanupOptions,
  log: string[],
  separatedTests: unknown[],
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
  aircraft.Tests = tests;
  return aircraft;
}

async function writeObjectFile(path: string, value: unknown): Promise<void> {
  await writeFile(path, JSON.stringify(value), "utf8");
}

async function writeDataset(
  outputPath: string,
  metadata: RecordValue,
  records: OutputRecord[],
  groupFiles: Map<string, string[]>,
  separatedTests: unknown[],
): Promise<void> {
  const output = createWriteStream(outputPath, { flags: "w" });
  const write = async (value: string) => {
    if (!output.write(value)) await once(output, "drain");
  };
  const meta = { ...metadata };
  delete meta.Aircraft;
  const currentSeparated = asItems(meta.AndreasSeparatedTests);
  if (separatedTests.length) meta.AndreasSeparatedTests = [...currentSeparated, ...separatedTests];
  const entries = Object.entries(meta);
  await write("{");
  for (const [index, [key, value]] of entries.entries()) {
    if (index) await write(",");
    await write(`${JSON.stringify(key)}:${JSON.stringify(value)}`);
  }
  if (entries.length) await write(",");
  await write('"Aircraft":[');
  let written = 0;
  for (const record of records) {
    if (written) await write(",");
    if ("path" in record) {
      const input = createReadStream(record.path);
      for await (const chunk of input) await write(Buffer.isBuffer(chunk) ? chunk.toString() : String(chunk));
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
        await write(JSON.stringify(keeper));
      }
    }
    written += 1;
  }
  await write("]}");
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
