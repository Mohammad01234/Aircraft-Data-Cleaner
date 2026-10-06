# CH-53G Tit-Data Cleaning & Analysis Toolkit

Octave scripts for reading, auditing, cleaning, and analyzing the CH-53G helicopter
rotor track-and-balance dataset (`CH-53G Tit_data.json`). The toolkit resolves data-entry
errors and duplicate records, merges test records that represent one continuous balancing
effort split across multiple JSON entries, and provides FFT-based and histogram-based
analysis of the cleaned data.

---

## Table of Contents

1. [Repository Structure](#repository-structure)
2. [Requirements](#requirements)
3. [Data Schema](#data-schema)
4. [Quick Start](#quick-start)
5. [Entry-Point Scripts](#entry-point-scripts)
6. [Cleaning Pipeline (`step1.m`) — Step by Step](#cleaning-pipeline-step1m--step-by-step)
7. [Merge Decision Framework](#merge-decision-framework)
8. [FFT Averaging & Comparison (`fftdataavg.m`)](#fft-averaging--comparison-fftdataavgm)
9. [Histogram Visualization (`Histogramm.m`)](#histogram-visualization-histogrammm)
10. [Helper Function Reference](#helper-function-reference)
11. [Missing Helpers to Add](#missing-helpers-to-add)
12. [Known Limitations & Assumptions](#known-limitations--assumptions)
13. [Recommended Next Steps](#recommended-next-steps)

---

## Repository Structure

```
.
├── readingjsonfile.m           # Exploratory script: loads and inspects the raw JSON
├── step1.m                     # MAIN SCRIPT: full cleaning pipeline (Steps 1-6 + export)
├── fftdataavg.m                # FFT harmonic averaging + per-test compliance comparison
├── Histogramm.m                # Flight-distribution histogram visualizations
│
├── iso2serial.m                 # ISO-8601 timestamp -> serial date number
├── getFieldOrEmpty.m            # Safe struct field access
├── normalizeFlights.m           # Fixes cell/struct-array + missing-field inconsistencies (Flights)
├── normalizeBalanceData.m       # Same, for BalanceData
├── normalizeTrackData.m         # Same, for TrackData
├── toItems.m                    # (see "Missing Helpers") Uniform per-element iteration helper
├── alignStructFields.m          # Aligns two struct arrays' fields before concatenation
├── ternary.m                    # (see "Missing Helpers") Inline if/else expression
│
├── findAircraftIdx.m            # Look up an aircraft by TailNumber
├── findTestIdxByDate.m          # Look up a test by Started/Ended date
├── getFirstFlight.m             # Chronologically first flight in a list
├── getLastFlight.m              # Chronologically last flight in a list
│
├── checkFlightCompliance.m      # Is a flight's BalanceData within Haupt/Heck limits?
├── checkSettingsChanged.m       # Did CurrentSettings change between two boundary flights?
├── checkAdjustmentImplemented.m # (superseded by checkSettingsChanged) exact-match comparison
├── flattenAdjustments.m         # Flattens Parts->AdjustmentTypes->AdjustmentPoints to a map
├── getCurrentSettingsMap.m      # A flight's actual physical settings, flattened
├── getRecommendedSettingsMap.m  # A flight's SolveResults recommendation, flattened
│
└── collectHarmonicAmplitudes.m  # Extracts FFT amplitude at a target harmonic/condition
```

---

## Requirements

- **GNU Octave** (developed/tested on 11.3.0, Windows build). No toolboxes beyond base
  Octave are required; `containers.Map` and `jsondecode`/`jsonencode` are used throughout.
- All `.m` files listed above must sit in the **same working directory** — Octave resolves
  each helper function by filename, so none of them can be renamed independently of their
  `function` declaration.
- The raw dataset file `CH-53G Tit_data.json` must also be in that same directory (or the
  filename inside each script's `fopen(...)` call must be updated to match its actual path).

---

## Data Schema

```
data
├── AircraftType, AmplitudeUnits, PhaseUnits, SpeedUnits, TrackUnits
├── Configurations[]              -- Revision, Issue, UniqueIdentifier, TestingTasks[]
└── Aircraft[]
    ├── TailNumber
    └── Tests[]
        ├── TestNumber, Started, Ended
        └── Flights[]
            ├── FlightNumber, Started, Ended, ConfigurationUsed
            ├── BalanceData[]     -- TestCondition, Part, Axis, Amplitude, Phase
            ├── TrackData[]       -- TestCondition, Part, TrackSplit, BladeHeights
            ├── CurrentSettings[] -- Parts[] -> AdjustmentTypes[] -> AdjustmentPoints[]
            ├── SolveResults[]    -- Adjustments[] -> Parts[] -> AdjustmentTypes[] -> AdjustmentPoints[]
            ├── FftData[]         -- FftRange, TestCondition, Part, Transducers[] -> Samples[]
            └── QuestionAnswers[]
```

- `BalanceData.Part` is `Haupt` (main rotor) or `Heck` (tail rotor).
- `BalanceData.Axis` is one of `Lateral`, `Vertikal`, `Radial`, `Axial`.
- `TestCondition` is one of `Bodenlauf`, `Hovern OGE`, `90 Knoten`, `110 Knoten`, `130 Knoten`.
- `FftData.FftRange` labels such as `"0-10T Harmonic"` (Tail rotor, 10 harmonics) or
  `"0-20R Harmonic"` (main Rotor, 20 harmonics) describe an evenly-spaced frequency sweep;
  there is **no explicit per-sample harmonic-order field** — the fundamental frequency must
  be derived as `max(Frequency) / N`.

**Known parsing quirk:** `jsondecode` inconsistently returns array-type fields (`Flights`,
`BalanceData`, `TrackData`, etc.) as either a struct array or a cell array, and individual
elements can be missing fields present on others. The `normalize*.m` and `toItems.m`
helpers exist specifically to make this safe to iterate over.

---

## Quick Start

```matlab
% 1. Explore the raw file structure first (optional but recommended)
readingjsonfile

% 2. Run the full cleaning pipeline
step1
% -> writes CH-53G_Tit-data_CLEANED.json in the working directory
% -> prints a full audit log (removed flights, merges, deletions, resolved/unresolved chains)

% 3. Run FFT analysis + compliance comparison on the cleaned data
fftdataavg

% 4. Generate flight-distribution histograms
Histogramm
```

---

## Entry-Point Scripts

| Script | Role |
|---|---|
| `readingjsonfile.m` | Exploratory/diagnostic script. Loads the raw JSON, prints top-level structure, computes basic counts (aircraft/tests/flights), and runs a series of audits: chronological order violations, duplicate tail numbers (exact and formatting-variant), duplicate tests/flights, and test/flight overlap detection. Used to *discover* the issues that `step1.m` then corrects — not part of the cleaning pipeline itself. |
| `step1.m` | **Main script.** Loads the raw JSON and applies the full cleaning pipeline (Steps 1–6, detailed below), then exports the result to `CH-53G_Tit-data_CLEANED.json`. |
| `fftdataavg.m` | Post-cleaning analysis script. Computes the fleet-wide average FFT amplitude at a target harmonic/condition, compares each test's own average against that baseline, and reports each test's compliance outcome (based on its last flight) alongside the FFT comparison. |
| `Histogramm.m` | Visualization script. Builds seven histograms showing the distribution of flights-per-test across the dataset, split by rotor part (Haupt/Heck) and by condition category (Bodenlauf-only vs. other conditions). |

---

## Cleaning Pipeline (`step1.m`) — Step by Step

### Step 1 — Remove empty flights
Any flight with **neither** `BalanceData` nor `TrackData` populated is removed; it
contributes nothing to downstream analysis.

### Steps 2a–2d — Manually confirmed structural corrections
Four specific cases, identified by manual inspection of printed per-aircraft timelines:

| Aircraft | Correction |
|---|---|
| `84+24` | Single-flight test (20.04.2021) absorbed into the 03.03.2021–12.05.2021 test, inserted after its 1st flight. |
| `84+34` | 3-flight test (31.08.2020) absorbed into the 31.08.2020–03.09.2020 test, inserted after its 3rd flight. |
| `84+64` | Last 2 real flights of the 30.11.2022–14.04.2023 test split into a new test, inserted after the 08.02.2023–09.02.2023 test; the original test's date range recomputed from its remaining flights. |
| `85+07` | 2nd flight of the 23.07.2022–18.10.2022 test deleted; `Ended` recomputed from the remaining flight. |

In every case, a test's `Started`/`Ended` is **recomputed from its actual member flights'
real timestamps** — never fabricated.

### Step 3a-pre — Exact-duplicate tail number resolution (`85+01`)
Two separate `Aircraft` entries were found sharing the identical tail number `85+01`. The
entry with exactly one test (the "donor") had its test inserted into the other entry's
list (positioned before its last test), and the donor entry was deleted.

> **Note:** an additional exact-duplicate case (`84+97`, appearing as two separate entries)
> was found later and requires the same fix pattern, applied *before* Step 3a runs (see
> [Known Limitations](#known-limitations--assumptions)).

### Step 3a — Formatting-variant duplicate merges
Six aircraft, confirmed via test-date-range overlap, represent the same physical aircraft
under two tail-number spellings: `84+39`/`8439`, `84+43`/`8443`, `84+91`/`8491`,
`84+97`/`8497`, `85+01`/`8501`, `85+03`/`8503`. All tests from both entries are combined,
sorted chronologically, and the "+"-formatted entry is kept.

### Step 3b — Junk entry removal
Non-aircraft entries `J` and `V85+00` are deleted.

### Step 3c — `85+06` / `8506` special case
Only the **last** (most recent) of `8506`'s three tests is kept and merged into `85+06`;
`8506`'s other two tests are discarded and the `8506` entry deleted.

### Step 4 — Targeted deletions
Specific erroneous/duplicate tests, identified by tail number + start date, are deleted:
`84+24` (14.12.2018, both matches), `84+44` (30.10.2019), `84+48` (21.01.2021), `85+03`
(25.01.2018–05.03.2018), and two tests for `84+51`. Two tests for `85+01` (starting
07.09.2023) are pulled into a separate holding field, `data.AndreasSeparatedTests`, rather
than deleted or left in place.

### Step 6 — Evidence-based test-chain merging
Replaces an earlier, looser pairwise-merge approach (see [Merge Decision
Framework](#merge-decision-framework)). Builds multi-test merge chains starting from any
test whose last flight is non-compliant, extending the chain only when strong evidence
supports it, and only committing the merge if the chain actually reaches compliance.

### Export
The final `data` structure — reflecting every step above — is serialized with
`jsonencode(data, 'PrettyPrint', true)` and written to `CH-53G_Tit-data_CLEANED.json`, in
the same schema as the original file.

---

## Merge Decision Framework

### The problem
A test exists to bring the aircraft's vibration within limits. If a test closes while its
last flight is still non-compliant, the very next chronological test may really be a
continuation of the same repair effort rather than an independent new event. The challenge
is distinguishing a genuine continuation from an unrelated later test.

### Compliance limits (`checkFlightCompliance.m`)

| Part | Axis | Limit |
|---|---|---|
| Haupt | Vertikal | 0.2 (fixed) |
| Haupt | Lateral | 0.3 before 2023-07-20, 0.25 from 2023-07-20 onward |
| Heck | Radial | 0.2 (fixed) |
| Heck | Axial | 0.2 (fixed) |

Applies identically across every `TestCondition`.

### The three continuity signals

1. **Same `ConfigurationUsed`** — no hardware change occurred between the two boundary
   flights.
2. **Settings actually changed, in the recommended direction** (`checkSettingsChanged.m`)
   — compares actual `CurrentSettings` between the two boundary flights, and requires at
   least one change to agree in direction with the earlier flight's own `SolveResults`
   recommendation. (An all-zero-reset artifact was found in one real case — Aircraft
   `85+10`, a 426-day gap with 14 changes but 0 direction-matches — which is why
   `matchDirCount > 0` is required, not just `changedCount > 0`.)
3. **Time gap ≤ 45 days** between the last flight's `Ended` and the next test's first
   flight's `Started`.

### The rule
**All three signals must hold simultaneously** for a chain to extend. This conjunctive
rule was adopted specifically because a looser "any one signal is enough" version was
found to produce implausible merges (the `85+10` case, spanning over a year).

### The three-case logic

- **Case 1 — Already compliant:** left untouched; neighbors not even inspected.
- **Case 2 — Still non-compliant after merging:** the chain keeps extending, re-checking
  all three signals at each step.
- **Case 3 — Resolved:** as soon as an extension results in a compliant last flight, the
  chain stops and the merge is finalized.
- **Unresolved:** if evidence fails at any point, the chain stops **without merging
  anything from that failed step onward**; the tests are left separate and logged for
  manual review.

### Merge mechanics
All flights from every test in a resolved chain are combined into one test record (fields
aligned via `alignStructFields.m` first). The surviving test keeps the **earliest** test's
`TestNumber`; its `Started`/`Ended` are recomputed from the true min/max flight timestamps.

---

## FFT Averaging & Comparison (`fftdataavg.m`)

1. **Global baseline:** flattens every flight in the dataset and computes the mean FFT
   amplitude at a target harmonic (`targetHarmonic`, default `1` = 1/rev) for a target
   `TestCondition` (default `'130 Knoten'`), separately for each of the four channels
   (Lateral, Vertikal, Radial, Axial), via `collectHarmonicAmplitudes.m`.
2. **Per-test comparison:** for each test, computes its own mean at the same
   harmonic/condition and compares it against the global baseline (`±10%` threshold for
   "close to fleet average" vs. "HIGHER"/"LOWER").
3. **Compliance outcome:** for the same test, checks whether its actual last flight
   (by real `Ended` timestamp, not array position) is within the Haupt/Heck limits, and
   prints either `CLOSED COMPLIANT` or `CLOSED WHILE STILL NON-COMPLIANT` with an itemized
   list of violated readings.
4. **Output filtering:** the script includes a two-pass structure — Pass 1 computes every
   test's full summary; Pass 2/3 can filter which summaries are actually printed (e.g. only
   the first N compliant tests, plus every non-compliant test and the one immediately
   following it) without changing the underlying computation.

**How the harmonic frequency is derived:** since no explicit per-sample "order" field
exists, `collectHarmonicAmplitudes.m` parses the `FftRange` label (e.g. `"0-10T Harmonic"`)
via regex to get `N`, computes the fundamental as `max(Frequency)/N`, and picks the sample
closest to `fundamental * targetHarmonic` (within half a bin-width).

---

## Histogram Visualization (`Histogramm.m`)

Builds seven histograms in one figure, each showing the distribution (across all tests) of
the number of flights-per-test meeting a specific criterion:

1. All flights per test
2. Haupt flights per test
3. Heck flights per test
4. Haupt, Bodenlauf-only flights per test
5. Haupt, other-condition flights per test
6. Heck, Bodenlauf-only flights per test
7. Heck, other-condition flights per test

A flight counts toward "Haupt" or "Heck" if any of its `BalanceData` or `TrackData`
entries name that part (Heck is checked in `BalanceData` only, since `TrackData` does not
carry Heck entries in this dataset). "Bodenlauf-only" means the flight's combined set of
conditions (across `BalanceData` + `TrackData`) is exactly `{'Bodenlauf'}`; "other"
means at least one of `Hovern OGE` / `90 Knoten` / `110 Knoten` / `130 Knoten` is present.

---

## Helper Function Reference

| File | Purpose |
|---|---|
| `iso2serial.m` | Converts an ISO-8601 timestamp string (handling variable-length fractional seconds) to an Octave serial date number for comparison/arithmetic. |
| `getFieldOrEmpty.m` | Returns `s.(fieldName)` if it exists, else `[]` — avoids "structure has no member" errors on inconsistent struct arrays. |
| `normalizeFlights.m` | Forces `Flights` into a consistent struct array: handles the cell-array case, fills in any struct's missing fields (union of all fields seen), and guarantees the 5 required fields always exist. |
| `normalizeBalanceData.m` / `normalizeTrackData.m` | Same normalization pattern, applied to `BalanceData` / `TrackData`. |
| `toItems.m` *(see below — not yet in repo)* | Converts any of the above into a plain cell array for element-by-element iteration without requiring matching struct fields (used where concatenation is not needed, only iteration). |
| `alignStructFields.m` | Makes two struct arrays share an identical field set/order so they can be safely concatenated with `[a; b]`. |
| `findAircraftIdx.m` | Returns the index (or indices) into `data.Aircraft` matching a given `TailNumber`. |
| `findTestIdxByDate.m` | Returns the index (or indices) into a `Tests` array matching a given `Started`/`Ended` date prefix. |
| `getFirstFlight.m` / `getLastFlight.m` | Return the chronologically first/last flight from a flight list, by actual timestamp — not by array position. |
| `checkFlightCompliance.m` | Checks a single flight's `BalanceData` against the Haupt/Heck limits (see table above); returns whether it's compliant, whether it had any relevant data at all, and a list of specific violations. |
| `checkSettingsChanged.m` | Compares `CurrentSettings` between two boundary flights directly; flags a genuine change only when at least one point's change direction agrees with the earlier flight's `SolveResults` recommendation. |
| `checkAdjustmentImplemented.m` | *(earlier/superseded approach)* Compares a flight's `SolveResults` recommendation against exact values in the next flight's `CurrentSettings`, requiring ≥80% match within tolerance. Kept for reference; `checkSettingsChanged.m` is the version used in Step 6. |
| `flattenAdjustments.m` | Flattens a `Parts[] -> AdjustmentTypes[] -> AdjustmentPoints[]` structure into a `Part\|AdjustmentType\|AdjustmentPoint -> Value` map. |
| `getCurrentSettingsMap.m` | Returns a flight's actual physical settings as a flattened map, merging all of that flight's `CurrentSettings` entries oldest-to-newest so a later partial update doesn't erase untouched points. |
| `getRecommendedSettingsMap.m` | Returns a flight's `SolveResults` recommendation (latest entry by `Created`) as a flattened map. |
| `collectHarmonicAmplitudes.m` | Extracts the FFT amplitude at a target harmonic order and `TestCondition`, per channel, from a set of flights (see FFT section above for the derivation method). |
| `ternary.m` *(see below — not yet in repo)* | Simple `if/else`-as-expression helper. |

---

## Missing Helpers to Add

Two helper functions are referenced throughout the codebase (`toItems` in
`fftdataavg.m`/`collectHarmonicAmplitudes.m`, and `ternary` in
`checkFlightCompliance.m`) but were not included in this upload batch. Add them as their
own `.m` files:

**`toItems.m`:**
```matlab
function items = toItems(x)
    % Returns a cell array of individual elements, regardless of whether x
    % is a cell array, a struct array, a scalar struct, or empty.
    % Unlike [x{:}], this never requires matching fields, since it doesn't
    % concatenate anything -- just wraps each element for safe iteration.
    if isempty(x)
        items = {};
    elseif iscell(x)
        items = x;
    else
        n = numel(x);
        items = cell(n, 1);
        for idx = 1:n
            items{idx} = x(idx);
        end
    end
end
```

**`ternary.m`:**
```matlab
function out = ternary(cond, a, b)
    if cond
        out = a;
    else
        out = b;
    end
end
```

---

## Known Limitations & Assumptions

- **`checkSettingsChanged.m` (Signal 2) has not been independently validated** against
  ground-truth maintenance records — it is inferred from the JSON structure and its
  self-consistency under testing. Spot-check a handful of resolved merges before treating
  the output as final.
- **The 45-day gap threshold is a judgment call**, not derived from documented policy.
- **A second exact-duplicate tail number case (`84+97`) was found after `step1.m` was
  already built** and is not yet reflected in the uploaded script — the same `3a-pre`-style
  fix used for `85+01` needs to be added for `84+97` (and a full sweep run afterward to
  confirm no further exact duplicates remain) before the export is trusted as final.
- **Unresolved non-compliant tests are intentionally left unmerged** — `step1.m`'s console
  output includes a full list of these; treat it as a manual-review queue, not a final
  determination.
- **`data.AndreasSeparatedTests`** (two tests pulled from `85+01`) sits outside the
  standard `Aircraft` array in the exported file — confirm its final disposition before
  delivery.
- **FFT harmonic extraction is a derived quantity**, not a raw field — it depends on
  correctly parsing the `FftRange` label, which was verified against the real file
  structure but is worth re-confirming if new `FftRange` label formats appear in a larger
  production file.
- **At full production scale**, `FftData.Transducers.Samples` can be extremely large (tens
  of millions of records in a ~300 MB sample seen during development). If the full file
  approaches that scale, `jsondecode` may struggle to load it entirely into memory;
  stripping `FftData` via a streaming pre-processor before loading into Octave is
  recommended for `step1.m`'s cleaning logic, since none of it depends on `FftData`.

---

## Recommended Next Steps

1. Add the `84+97` exact-duplicate fix to `step1.m` (before Step 3a) and re-run a full
   tail-number-duplicate sweep to confirm no others remain.
2. Add `toItems.m` and `ternary.m` to the repository (see above).
3. Manually audit the "unresolved non-compliant tests" list from Step 6's output.
4. Validate `checkSettingsChanged.m` against any available known-good repair records.
5. Decide the final disposition of `data.AndreasSeparatedTests`.
6. Re-run Step 6 with the gap threshold varied (e.g. 30/45/60 days) to test sensitivity.
7. If moving to the full production file, benchmark load time/memory before assuming
   `step1.m` will run as-is; consider a pre-processing step to strip `FftData` if not
   needed for a given run.
