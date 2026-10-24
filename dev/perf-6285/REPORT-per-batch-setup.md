# #6285 follow-up dart — per-batch expression setup (evidence only)

Code: `dev/perf-6285/per-batch-setup/0001-*.patch`, applies to main 418e8b4 + harness (`708148c`, then the refresh commit `b595eec` on top).
Pushed as a patch on `dart/6285-sel-aware-vectors` rather than a new `dart/6285-per-batch-setup` branch: this session may only push its configured branch.

## TL;DR

- Goal reached for predicates and projections: the compiled fn is resolved once and reused while the input var types, param types (or the identical `args` object) and zone are unchanged — per batch the check is one `getType` per *referenced* var plus an `Arrays.equals`, no map building, no `->param-types`, no memo hashing.
- Selection specs now emit `(fn [rel schema args] int[])` — the generated loop writes matching row indices straight into an `int[]` (`Arrays.copyOf` to size), replacing bool vector → `VectorMask` → `IntStream` → `int[]`.
- **Q6 −11%, Q1 −4% (Q1 within noise)** on the refreshed-snapshot layout (#6174), 3 interleaved JVMs each. Bigger win than anything in the sel-aware avenue.

## Change

- `->emitted-resolver` (new, `xtdb.expression`): `(fn [in-rel args] emitted)`, caching `[args param-types var-type-array zone emitted]` in one volatile (swapped whole: specs can be shared by concurrent cursors through the plan cache, so readers must see a consistent entry).
  - hit: `(identical? zone)`, `args` identical **or** `->param-types` equal, per-var `VectorType`s `Arrays.equals` the cached ones.
  - miss: builds the narrowed var-types map and goes through the existing `emit-projection` lru-memoize as before.
  - Only active with narrowed var-types (orchestrator: treat as landed); the wide path is unchanged.
- `->expression-projection-spec` uses it.
- `emit-selection` (new, lru-memoized like `emit-projection`) + `->expression-selection-spec` uses it with the resolver. `->expression-mask-spec` (group-by FILTER) still goes through the projection spec, which is now cached.
- Toggles: `-PnoSpecCache`, `-PnoDirectSel` (old selection path).

## Numbers

SF1 RA, 50 hot reps/JVM, drop 20, interleaved base/pbs, **refreshed snapshot** (`:refresh-snapshot` stage, harness `b595eec`). Baseline = main + harness + `-PnarrowVarTypes`. `runs.py --drop 20` verbatim:
```
pr-base-1.log        q1   cold  97519 n  30 median    5569 mean    5576 min   5291 sd    165
pr-base-1.log        q6   cold   4561 n  30 median    4140 mean    4141 min   3845 sd    136
pr-base-2.log        q1   cold   8558 n  30 median    6400 mean    6444 min   6142 sd    245
pr-base-2.log        q6   cold   4194 n  30 median    4238 mean    4254 min   4048 sd    133
pr-base-3.log        q1   cold   7485 n  30 median    6086 mean    6103 min   5673 sd    176
pr-base-3.log        q6   cold   4392 n  30 median    4358 mean    4344 min   4007 sd    143
pr-pbs-1.log         q1   cold   7000 n  30 median    5289 mean    5324 min   5106 sd    169
pr-pbs-1.log         q6   cold   3979 n  30 median    3543 mean    3547 min   3317 sd    116
pr-pbs-2.log         q1   cold   9785 n  30 median    5548 mean    5561 min   5281 sd    159
pr-pbs-2.log         q6   cold   4245 n  30 median    3687 mean    3700 min   3456 sd    138
pr-pbs-3.log         q1   cold   8102 n  30 median    6516 mean    6536 min   6165 sd    195
pr-pbs-3.log         q6   cold   3952 n  30 median    4080 mean    4028 min   3729 sd    143
```

Mean of per-JVM medians (ms): Q1 base 6018 → pbs 5784 (−3.9%; pairwise −5%, −13%, +7%); Q6 base 4245 → pbs 3770 (−11.2%; pairwise −14%, −13%, −6%).
The box is noisy (base Q1 medians 5569–6400 across three JVMs); Q6 is consistent across all three pairs, Q1 isn't.
Cold Q1 ~97s on the first run after a refresh (base-1) — the re-plan over the new layout, not this change (seen once on the pbs quick run too).

Stale-layout (no refresh) indication only — a planned 3×3 base/pbs/cache-only run was lost (the nohup'd driver died with its shell): quick 3-rep run gave hot Q1 5.5 s / Q6 3.9 s vs base ~6.1 / 4.6 at that time of night.
So the cache-only vs direct-`int[]` split was **not** measured; JFR of pbs not taken either.

Prior JFR (base, stale layout) for the parts this removes: per-batch setup 3.6% Q1 / 9.6% Q6; `VectorMask.open` + IntStream 1% Q1 / 3% Q6 — matches the Q6 win being larger.

## Tests

- `:test --tests 'xtdb.operator*' 'xtdb.expression*' 'xtdb.tpch*' 'xtdb.sql*'`: pass with narrowed var-types off (default) **and** forced on (temporarily defaulted in source, reverted). All bench runs passed the pinned-result check.
- Not run: full suite, integration, property.

## Data structures that matched

- One immutable vector in a volatile as the cache entry (atomic swap), keyed by identity first (args, zone) and by cheap equality second (per-var `VectorType` array).
- `int[]` written from the generated loop: the selection's natural output; no intermediate Arrow allocation per batch.

## Suggested sub-goals

1. **tidy**: extract the "resolve emitted fn for (var-types, param-types, zone)" step out of `project` into a resolver closed over by the spec (no behaviour change; still re-resolves every batch).
2. **advance (perf)**: resolver caches the last entry (identity/equality check). Depends on narrowed var-types landing, and on 1.
3. **advance (perf)**: `emit-selection` — selection specs return `int[]` from generated code. Independent of 1–2; can share the resolver once 2 lands.
4. Later: MaskSpec (group-by FILTER) could get the same direct-mask emission.

## Questions / risks / assumptions

- Spec instances are shared via the plan cache: the cache is one volatile swap, so a race only costs an extra emit — but confirm no caller relies on `project` re-reading `*default-tz*`-dependent state other than via the zone check.
- `args` identity: per-row args (apply/correlated subqueries) miss the identity check and fall back to `->param-types` + equality — correct, but pays the old cost for those.
- Assumes `VectorType` equality is cheap (data classes; `Maybe` allocates per `getType` but compares shallowly).
- Error semantics: the selection path now catches the same exception classes as projection; it previously surfaced them through the projection spec — same anomalies.

## Dead-ends

- None in code; the lost 3×3 stale-layout run is the gap (driver launched with `nohup … &` inside a tool call died with its shell — use the tool's background mode).
