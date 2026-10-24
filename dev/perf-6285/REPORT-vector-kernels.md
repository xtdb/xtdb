# Avenue D — built-in vector kernels instead of generated expressions (#6285 / #4969)

Branch `dart/6285-vector-kernels` (dart; not for landing).
Base: xtdb/xtdb main 418e8b4 + harness 5d57965.
Couldn't read #6285/#4969/#6163/#4964: this session only has `jarohen/xtdb` attached, and a same-named `xtdb/xtdb` can't be added alongside it. I worked from the brief.

## What was built

- **`RangePredicate`** (`core/.../expression/kernel/Kernels.kt`) covers `lo (<|<=) col (<|<=) hi`, with either bound optional and each bound a param or a literal.
  It runs on `DoubleVector` (f64) and `DateDayVector` (date-day, compared as int epoch days).
  It reads `dataBuffer`/`validityBuffer` directly, and takes an optional input selection `int[]`, returning the surviving subset.
  The date bounds are folded to an inclusive `long` range once per batch.
- **`DoubleProjectionKernel`** does lanewise f64 `+ - *` over f64 columns and numeric literals: it gathers each column to a `double[]`, runs one pass per op (reusing the temporaries), and bulk-writes into a new `DoubleVector`.
  Its nulls are the OR of the inputs' validity.
- **Unwrapping**: `unwrap(VectorReader)` peels `IndirectVector` (Selection/Slice/other) and `RenamedVector` down to the physical vector, returning `phys(r) = idxs?[r] + offset`.
  For a single indirection it reuses the `int[]` rather than composing it.
  `IndirectVector.inner/sel` and `RenamedVector` were widened to `internal`/public for this.
- **Dispatch**: both are pattern-matched in Clojure, on the `form->expr` output (pre-`prepare-expr`):
  - `->expression-selection-spec` matches `(cmp var scalar)` either way round, or an `(and …)` of those on one var with at most one lower and one upper bound.
    It wraps the codegen spec in `KernelSelectionSpec(pred, fallback)`.
  - `->expression-projection-spec` matches a `+ - *` tree of vars and numeric literals with at least one var, and wraps it in `KernelProjectionSpec(kernel, fallback)`.
  - The decision is static (at plan time) on the expression shape.
    At run time the kernel returns `null` when a vector isn't the expected physical class (DUV, i64 column, timestamp param…), and the codegen fallback runs instead.
    Under `-PkernelsDebug` it logged matches and fallbacks: all six Q1/Q6 shapes matched, with zero fallbacks.
- **Composed scan selection**: `SelectionSpec.selectWithin(al, rel, inSel, schema, params)` defaults to select-then-compose; the kernel overrides it.
  `ScanCursor` now threads one `int[]` through all the column predicates and calls `rel.select(sel)` once, instead of once per predicate (`-PcomposeSelections=false` reverts).
- **Toggles**: `-Pkernels=all|sel|proj|none`, `-PkernelsDebug` (match/fallback log and call/row counters).

Constant subexpressions: none in the RA Q1/Q6 plans.
`?ship_date` etc. arrive precomputed as params (`LocalDate`, `0.05`…), so no `date + interval` folding was needed.
The kernel resolves each param once per batch (`args.vectorForOrNull(name).getObject(0)`).

## Numbers

SF1, RA Q1/Q6, `-PnarrowVarTypes` on both sides, results verified by the bench (no mismatch in any run).

### Round 1 — kernels v1, 3+3 JVMs interleaved B K B K B K, 50 reps, drop 20
```
base-1.log           q1   cold  11839 n  30 median    8473 mean    8456 min   7954 sd    289
base-1.log           q6   cold   7023 n  30 median    6302 mean    6279 min   5794 sd    257
base-2.log           q1   cold  11720 n  30 median    8636 mean    8667 min   8111 sd    318
base-2.log           q6   cold   6954 n  30 median    6288 mean    6338 min   5660 sd    365
base-3.log           q1   cold  11208 n  30 median    7930 mean    7967 min   7546 sd    249
base-3.log           q6   cold   6526 n  30 median    5762 mean    5805 min   5484 sd    205
kern-1.log           q1   cold  10448 n  30 median    8278 mean    8330 min   7747 sd    351
kern-1.log           q6   cold   5280 n  30 median    5412 mean    5461 min   4956 sd    276
kern-2.log           q1   cold  11251 n  30 median    7731 mean    7757 min   7176 sd    273
kern-2.log           q6   cold   5568 n  30 median    5122 mean    5135 min   4835 sd    150
kern-3.log           q1   cold  10634 n  30 median    7891 mean    7864 min   7290 sd    290
kern-3.log           q6   cold   5219 n  30 median    5215 mean    5236 min   4825 sd    250
```
- Q1 mean of per-JVM medians 8346 → 7967 ms (**−4.5%**); Q6 6117 → 5250 ms (**−14.2%**).

### Round 3 — final code (v1 + no-copy `Physical`), 3+3 JVMs interleaved, 50 reps, drop 20
```
base-1.log           q1   cold  10795 n  30 median    8386 mean    8393 min   7842 sd    352
base-1.log           q6   cold   6565 n  30 median    6078 mean    6055 min   5601 sd    220
base-2.log           q1   cold  10484 n  30 median    8130 mean    8204 min   7749 sd    300
base-2.log           q6   cold   6132 n  30 median    5972 mean    6100 min   5646 sd    464
base-3.log           q1   cold  11007 n  30 median    8402 mean    8469 min   7853 sd    411
base-3.log           q6   cold   6100 n  30 median    6164 mean    6178 min   5500 sd    360
kern-1.log           q1   cold  10939 n  30 median    7694 mean    7735 min   7392 sd    240
kern-1.log           q6   cold   4915 n  30 median    4818 mean    4860 min   4588 sd    198
kern-2.log           q1   cold  10021 n  30 median    7860 mean    7920 min   7316 sd    329
kern-2.log           q6   cold   5157 n  30 median    5105 mean    5130 min   4721 sd    254
kern-3.log           q1   cold  11119 n  30 median    8272 mean    8269 min   7637 sd    262
kern-3.log           q6   cold   5835 n  30 median    5414 mean    5411 min   4875 sd    259
```
- Q1: median of per-JVM medians 8386 → 7860 ms (**−6.3%**); per-JVM base [8386, 8130, 8402] vs kernels [7694, 7860, 8272].
- Q6: 6078 → 5105 ms (**−16.0%**); per-JVM base [6078, 5972, 6164] vs kernels [4818, 5105, 5414].
- The kernels' worst JVM beats the base's best on Q6; on Q1 the ranges overlap (kern-3 8272 vs base-2 8130).

### Round 2 — ablation, one JVM each, 35 reps, drop 15 (JVM-to-JVM noise is ±4%, so read these as indicative)
```
base.log             q1   cold  11936 n  20 median    8470 mean    8576 min   8069 sd    322
base.log             q6   cold   6964 n  20 median    6267 mean    6252 min   5673 sd    190
none.log             q1   cold  10623 n  20 median    8722 mean    8761 min   8246 sd    250
none.log             q6   cold   5746 n  20 median    6474 mean    6461 min   6094 sd    203
sel.log              q1   cold  10833 n  20 median    8258 mean    8270 min   7938 sd    251
sel.log              q6   cold   5488 n  20 median    5124 mean    5141 min   4889 sd    209
proj.log             q1   cold  11841 n  20 median    8216 mean    8226 min   7577 sd    309
proj.log             q6   cold   6853 n  20 median    6063 mean    6008 min   5484 sd    253
all.log              q1   cold  10319 n  20 median    7472 mean    7494 min   6949 sd    300
all.log              q6   cold   5015 n  20 median    4974 mean    5013 min   4610 sd    311
plainmain.log        q1   cold  11522 n  20 median    8346 mean    8374 min   7968 sd    314
plainmain.log        q6   cold   6646 n  20 median    6110 mean    6126 min   5659 sd    289
```
| variant | Q1 median | Q6 median |
|---|---|---|
| base (main+harness, narrowed var-types) | 8470 | 6267 |
| plain main+harness (no narrowing) | 8346 | 6110 |
| `none` — composed scan selection only, no kernels | 8722 | 6474 |
| `sel` — range-select kernels only | 8258 (−2.5%) | 5124 (−18%) |
| `proj` — f64 projection kernel only | 8216 (−3%) | 6063 (−3%) |
| `all` | 7472 (−12%) | 4974 (−21%) |

- Composing the scan's selections without kernels buys nothing (it's within noise, if anything slightly worse).
  At these batch sizes, re-indirecting ~5 columns per predicate isn't the cost.
- Narrowed var-types vs plain main isn't distinguishable in this round (single JVM each).

### Per-shape (JFR, one JVM each, 15 reps; `analyse_stream.py`, samples under `ScanCursor.tryAdvance`)
| phase | baseline | kernels |
|---|---|---|
| total samples | 12456 | 9577 (−23%) |
| predicate/projection codegen evaluation | 2305 (18.5%) | 7 |
| per-batch param-types / zone setup | 1071 (8.6%) | 0 |
| mask → int[] (`VectorMask`/`IntStream`) | 372 (3.0%) | 0 |
| kernel: range select | – | 581 (6.1%) |
| kernel: f64 projection | – | 501 (5.2%) |
| select/indirection compose | 240 | 94 |
| ProjectCursor lambda | 208 | 208 |
| event merge (EntityMerge/AsOfResolver/PQ) | 3243 | 3549 |
| group-by | 3828 | 3538 |
| page load | 796 | 782 |

- The EE-attributable slice goes from ~4200 to ~1400 samples, about **3×** cheaper.
  Most of the saving is the per-batch fixed cost (the param-types/zone setup on every `project` call, the projection-fn cache lookup, the boolean vector → `VectorMask` → `IntStream` → `int[]`), not the per-row arithmetic.
- Selection is the bigger win per shape: Q6 is 3 range predicates with nothing else to amortise, and its gain is almost all `sel`.
  Q1's single `<=` passes ~98% of rows, so its win is split between `sel` and the two projections.
- What's left is event merge (~37%) and group-by (~37%) — not the EE's.

### Batch size — the finding that shapes everything else
`-PkernelsDebug` counters over cold + 2 hot runs of Q1+Q6: **525,723 select calls for 39.5M rows (~75 rows/call); 377,451 projection calls for 35.8M rows (~95 rows/batch)**.
- The SF1 table after the harness's load has l00 ×59, l01 ×59, l02 p0–p3 files.
  `ScanCursor` emits one relation per leaf page per merge task (`BitemporalConsumer.build` → `rels.map`), so batches are tiny.
- At ~90 rows/batch, per-batch constant cost dominates any kernel: the measured kernel cost works out at ~25–50 ns/row, against ~1–2 ns/row for the same loops in isolation.
  This is why the wins come from deleting per-batch EE machinery, and why SIMD doesn't matter yet.

### Vector API (`dev/perf-6285/VecApiBench.java`, `java --enable-preview --source 21 --add-modules jdk.incubator.vector`, AVX-512 species, 8 lanes)
| ns/row | N=1M | N=4096 |
|---|---|---|
| range select, branchy scalar | 7.1 | 0.9 (predictor memorises the repeated batch) |
| range select, branchless scalar | 1.3 | 1.0 |
| range select, Vector API compare→mask→`toLong` bit-walk | 1.7 | 0.43 |
| `p*(1-d)*(1+t)` fused scalar (auto-vectorised) | 1.9 | 0.41 |
| same, Vector API fused | 1.95 | 0.63 |
| same, lanewise (one pass per op, temporaries, as the kernel does) | 7.3 | 1.6 |
| gather off-heap `double` via `int[]` | 0.8 | 0.46 |

- **The Vector API doesn't pay for projections**, where C2 already auto-vectorises the fused scalar loop.
  It pays ~2.3× for compare→selection at cache-resident sizes, but against batches of ~90 rows and a query costing ~1 µs/row end-to-end, that's noise.
  It isn't worth the incubator module and the `--add-modules` on every JVM until batches are ≥ thousands of rows.
- **Lanewise-with-temporaries is ~4× slower than fused**: the column-at-a-time composition costs memory traffic.
  A kernel library should therefore offer fused shapes (`a*(1-b)`, `a*(1-b)*(1+c)`), or fuse a small tree into one loop, rather than chaining one op per pass.
  Again irrelevant at current batch sizes.

### Tests (kernels `all` by default)
- `:test` (1863 tests) and `:xtdb-core:test` (601): **0 failures**, run with the 8 Docker-dependent test namespaces (and their dependents) moved aside, since `compileTestClojure` can't load them without Docker.
- Includes `xtdb.operator.*`, `xtdb.expression*`, `xtdb.tpch-test`.
- I added no kernel-specific tests (dart), so the null / NaN / type-fallback paths are only covered as far as the existing suites reach them.

## Data structures that matched the problem
- **An `int[]` selection threaded through the predicates (`selectWithin(inSel)`)**: each predicate only looks at survivors, and the relation is re-indirected once.
  It's simple, composes with the existing `IndirectVector`, and needs no new vector type.
- **`(base vector, idxs?, offset)` as the kernel's view of a column**: one indirection level, resolved once per batch.
  Every scan column arrives as `IndirectVector(put-struct child, bitemporal selection)`, so the kernel always gathers anyway.
- **Folding bounds to a closed `long` range for date-days** (`lo+1` / `hi-1` for exclusive bounds): one compare shape per type.
- **Direct `ExtensibleBuffer.getDouble/getInt` and `BitBuffer` validity**, with no `getDouble0` null-check.
- **Bulk `DoubleVector.writeDoubles(double[], n)`**.

## Data structures that didn't
- **The boolean vector → `VectorMask` (bit buffer) → `IntStream.Builder` → `int[]` chain** in `->expression-selection-spec`: three representations per batch to produce one `int[]`.
  A bitmask is only worth it if the consumer takes a mask; the scan's consumer wants an `int[]`.
- **Lanewise ops over `double[]` temporaries**: fine at 90 rows, but ~4× worse than fused at scale.
- **`HashMap<String, Physical>` per projection batch**: visible in the profile at 90-row batches.
  Positional arrays resolved at spec-build time would be better.
- **Bitmask selection (#4964) for the scan predicates**: at ~25% selectivity on Q6's second predicate, and 90-row batches, an `int[]` of survivors is cheaper to iterate than re-scanning a mask.
  A mask would pay for wide batches with high selectivity (Q1's 98%), where the mask avoids writing `int[]` and lets the next predicate run lanewise on contiguous data.

## Suggested sub-goals
1. **tidy** — give `SelectionSpec` a `selectWithin(…, inSel)` and have `ScanCursor` thread one selection through the column predicates, calling `rel.select` once.
   It's behaviour-neutral (default = select + compose), depends on nothing, and is the seam both kernels and #4964 need.
2. **tidy** — expose a "physical view" of a column (`base, idxs?, offset`) from `VectorReader`.
   Today that takes `internal` access to `IndirectVector.inner/sel` and `RenamedVector`, which are themselves widened.
   A method on `VectorReader` (default: `null` = not a fixed-width physical) avoids poking private state.
   Depends on nothing.
3. **advance** — add range-select kernels for f64 and date-day columns, with param/literal bounds, dispatched in `->expression-selection-spec` and falling back to codegen.
   Depends on 1 and 2.
   This is the bulk of the Q6 win.
4. **advance** — add an f64 `+ - *` projection kernel, dispatched in `->expression-projection-spec`, with codegen fallback.
   Depends on 2.
   Its own win is ~3%; most of it is avoiding per-batch EE setup.
5. **advance** (separate) — **make batches bigger**: coalesce the scan's per-page relations per merge task (or fix whatever leaves the SF1 table in ~90-row pages).
   This multiplies the value of everything above and of the other avenues, and needs its own investigation.
   It's independent of 1–4.
6. **tidy** — move the per-batch work in codegen'd projection/selection (param-types, zone, cache key from `var-types`) to once per spec/args.
   That's the part of the kernel win that codegen could also have, without kernels.
   Independent of the others, and a good control experiment for how much of the kernel win is really "kernel".

## Questions
- Q-batch: why are the scan's batches ~90 rows at SF1 — is that expected of the current compaction layout (l00/l01 ×59 + l02 p0–p3 after the harness's `compact` stage), or should the hot path be seeing 1024-row pages?
  It blocks any judgement of SIMD/bitmask work, and sizes the value of every avenue.
- Q-scope: should kernels be keyed by physical vector class at run time (as here), or by `VectorType` at plan time?
  Run-time keying is what lets a kernel fall back on DUVs and nullable variants; plan-time keying would let the planner drop codegen entirely for covered shapes.
  It blocks sub-goals 3–4's dispatch design.
- Q-null: are TPC-H columns ever nullable in practice here?
  Both kernels handle validity, but the projection path with nulls (per-row `writeNull`) hasn't been exercised by Q1/Q6.

## Assumptions, risks, invariants touched
- **NaN**: the kernel compares with IEEE `>= <=` (NaN never selected), which matches codegen's primitive `<` for `:num`.
  An absent bound becomes ±Inf with `incl=true`, so NaN is excluded there too — consistent with codegen, which only tests the present bound.
- **Mixed numeric types**: literals are coerced to double, and so are integer params against f64 columns.
  A `decimal`/ratio literal isn't matched.
  A date column against a timestamp param, or an i64 column, falls back to codegen at run time.
  An i64 column with a double literal (e.g. `p_size < 24.0`) isn't kernelled at all.
- **Null param** → empty selection (SQL `col < NULL` is unknown).
  A missing param vector (`vectorForOrNull` null) is treated as unbounded rather than an error — that's a risk if a plan ever omits a param.
- **Arrow ownership**: the kernels allocate only `DoubleVector` outputs, which the caller owns as with codegen, and close them on throw.
  The selection kernels allocate no Arrow memory.
  No leak reports in the test run.
- **`RelationReader`/`IndirectVector` contract**: the kernel reads through `IndirectVector.sel`'s concrete `Selection`/`Slice`, and relies on the base being a `FixedWidthVector` subclass whose `dataBuffer` index = row.
  Nested indirections are composed in `unwrap`.
  `RenamedVector` is unwrapped.
  Anything else (DUV legs, struct children of a DUV, `ConformingUnloader` views) returns `null` → codegen.
- **Visibility**: `IndirectVector.inner/sel` became `internal` and `RenamedVector` became public — fine for a dart, not for landing (see sub-goal 2).
- **Overflow/decimal**: f64 only, so no overflow exceptions to preserve.
  i64 kernels would need `Math.multiplyExact` to match codegen's overflow errors, and decimal kernels must keep scale/precision rules.
- **Full TPC-H coverage**: the RA plans use ~7 kernel families — range compare (done); `==`/`<>`/or-of-`==` on utf8 and i64; LIKE prefix/suffix/contains (± `not`); f64 `+ - *` (done) plus `/`; `extract YEAR` from date; `if(pred, arith, 0)` masked blend; `substring`.
  At 2–4 physical types each, that's roughly 15–20 typed kernels.
  The join `:select` conditions and Q19's big or-of-ands would still be codegen.

## Dead-ends
- **Vector API**: closed — see above.
  No gain over auto-vectorised scalar for projections, a ~2× gain on compare→select that's invisible at ~90-row batches, and it needs `--add-modules` + `--enable-preview` (FFM) on JDK 21.
- **Composing selections alone** (`-Pkernels=none`): no measurable gain, so #6285's "compose once" isn't where Q1/Q6 time goes at this batch size.
  It's still the right seam for kernels.
- **Optimising the kernels' inner loops further** (branchless selection, fused projection): ~1 ns/row vs ~25–50 ns/row observed per-batch-amortised cost, so not where the time is until batches grow.
