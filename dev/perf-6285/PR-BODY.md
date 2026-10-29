Part of #6285. Dart record, closed unmerged: built-in vector kernels make the hot TPC-H Q1/Q6 scan predicates and projections cheaper (Q6 −16%, Q1 −6 to −7%), and bigger batches only help once copying rows into them is cheap.

- **Kernels win by deleting per-batch expression-engine work, not by faster arithmetic**
  Scan batches are ~75–244 rows, so the fixed cost of each generated-code call (param-type/zone setup, boolean vector → mask → `IntStream` → `int[]`) dominates the per-row loop.

- **Bench numbers taken on a restarted node before the refresh stage measured a stale layout**
  A restarted node plans from the snapshot cached at the last block (#6174): each merge task read 43 page slices (38 × L0 + 5 × L1) instead of 6 (4 × L1 + 2 × L2).

- **Coalescing a merge task's pages into one batch doesn't pay with today's row copier**
  The copy (one `ArrowBuf.setBytes` per row per column) costs as much as the per-batch overhead it removes.

- **The Vector API buys nothing measurable here**
  C2 already auto-vectorises the fused scalar projection, and selection's ~2× micro-gain is invisible at these batch sizes.

## What changes

None of this is meant to land; the branch is the dart's record. Base is main 418e8b4 plus the shared Q1/Q6 harness commit.

- **Both kernels fall back to generated code at run time**
  The match is on expression shape at plan time; a vector that isn't the expected physical class (a union, an i64 column, a timestamp param) returns null and the wrapped codegen spec runs. All six Q1/Q6 shapes matched with zero fallbacks.

- **The scan can coalesce batches, behind `-Pcoalesce`**
  `pre` copies a merge task's per-page relations into one batch before the predicates; `post` accumulates predicate survivors into ≥1024-row batches.

## Consequences

Full numbers, `runs.py` output and JFR tables are in `dev/perf-6285/REPORT-vector-kernels.md` and `dev/perf-6285/REPORT-batch-size.md` on the branch. SF1, RA Q1/Q6, `-PnarrowVarTypes` on both sides, three interleaved JVMs per variant, every result verified against the pinned results.

- **Kernels are the only variant that beats base on the realistic layout**
  On the refreshed layout (median of per-JVM medians):

  | variant | Q1 | Q6 |
  |---|---|---|
  | base | 7548 ms | 5112 ms |
  | kernels | −7.4% | −15.6% |
  | coalesce `pre` | +6.0% | +2.1% |
  | `pre` + kernels | −0.7% | −4.9% |

  On the stale layout kernels gave Q1 −6.3%, Q6 −16.0%. Single-JVM ablation there put almost all of Q6's gain on the selection kernels, and split Q1's between selection and projection.

- **The expression-engine slice of the scan profile drops about 3× with kernels**
  ~4200 → ~1400 samples on the stale layout. Generated-code evaluation, per-batch param-type/zone setup and the mask → `int[]` chain go to ~0; the kernels themselves take ~11%. What's left is event merge and group-by, ~37% each.

- **Base spends 12.7% of scan samples in `VectorIndirection.get` and selection composition on the refreshed layout**
  Generated code reads every column through an `IndirectVector`, and each predicate re-selects every column. Kernels avoid it by resolving one `int[]` per batch; `pre` coalescing avoids it by paying for a copy instead.

- **Composing predicates into one selection is not a win on its own**
  `-Pkernels=none` measured within noise of base. It's still the seam the kernels and #4964 need.

- **The shape of a lineitem scan depends on whether the snapshot was refreshed**

  | | stale (#6174) | refreshed |
  |---|---|---|
  | merge tasks per scan | 1024 | 4096 |
  | pages per task | 43 (38 × L0 + 5 × L1) | 6 (4 × L1 + 2 × L2) |
  | rows per emitted batch, pre-predicate | ~136 | ~244 |
  | hot Q1 / Q6, base | ~8.0 / 6.05 s | ~7.5 / 5.1 s |

  The PriorityQueue in `EntityMerge` was a 43-way merge on the stale layout and is 6-way refreshed, where event merge is ~17% of scan samples.

- **The first refreshed run's cold Q1 took 85–95 s; later JVMs took 9–12 s**
  The refreshed plan reads L1/L2 files the box hadn't read since writing them, so this looks like the OS page cache filling on first touch. Not confirmed by dropping caches.

- **Sub-goals, in dependency order**
  - **S1 tidy: `SelectionSpec.selectWithin(inSel)`, with `ScanCursor` composing its predicates into one selection**
    Behaviour-neutral; depends on nothing.
  - **S2 tidy: a physical view of a column (base vector, `int[]?`, offset) on `VectorReader`**
    Replaces the dart's widened `IndirectVector.inner/sel` and `RenamedVector` visibility; depends on nothing.
  - **S3 advance: range-select kernels for f64 and date-day columns, with codegen fallback**
    Depends on S1 and S2; most of the Q6 win.
  - **S4 advance: an f64 `+ - *` projection kernel, with codegen fallback**
    Depends on S2.
  - **S5 tidy: hoist the per-batch param-type, zone and cache-key work in generated projection/selection to once per spec**
    Independent; it's also the control for how much of the kernel win generated code could have without kernels.
  - **S6 advance: a typed gather-copy for fixed-width `copyRows`**
    One loop of 8-byte loads/stores over the `int[]`. Independent; it's the prerequisite for any coalescing, and sort, join build and group-by also copy through `rowCopier`.
  - **S7 advance: re-measure `pre` coalescing, alone and with kernels**
    Depends on S6.
  - **S8 tidy: keep `-PscanDebug`'s shape histograms as a permanent cursor metric**
    Independent; it's what exposed the stale layout.

## Alternative approaches

- **The Vector API (`jdk.incubator.vector`): microbenchmarked, not adopted (JDK 21, AVX-512)**
  Fused projection: 1.95 ns/row vs 1.9 for auto-vectorised scalar. Compare → mask → selection: 1.7 vs 1.3 ns/row for branchless scalar at 1M rows, 0.43 vs 1.0 at 4096. At ~90–244-row batches and ~1 µs/row end to end, neither shows up, and it needs `--add-modules` (plus `--enable-preview` for FFM) on every JVM.

- **One pass per op over temporaries: kept for the dart, measured ~4× slower than a fused loop at scale**
  7.3 vs 1.9 ns/row at 1M rows, 1.6 vs 0.4 at 4096. It doesn't matter at today's batch sizes, but a kernel library should offer fused shapes or fuse a small tree into one loop.

- **`post` coalescing (accumulate survivors): tried and dropped**
  The predicates still pay per-page cost, and Q1 copies 98% of its rows anyway; single-JVM runs showed no gain even on the stale layout.

- **A bitmask selection (#4964) for scan predicates: reasoned against for this workload**
  At ~25% selectivity on Q6's second predicate and ~90–244-row batches, iterating an `int[]` of survivors is cheaper than re-scanning a mask. A mask would pay on wide batches with high selectivity, like Q1's 98%.

- **A virtual concatenation of per-page relations: not tried**
  #4968's `ConcatVector` showed a per-row child lookup is slow, so the dart copied instead.

## Open questions

- **Q1: should kernels be chosen by physical vector class at run time, or by `VectorType` at plan time?**
  Run-time keying is what lets a kernel fall back on unions and nullable variants; plan-time keying would let the planner skip codegen entirely for covered shapes. Blocks the dispatch design for S3 and S4.

- **Q2: should coalescing live in the scan, or in a general re-batching operator the planner can insert?**
  The scan knows when a merge task's pages are done; a generic operator would also help joins and unnest. Blocks S7's placement.

- **Q3: are any TPC-H columns nullable in practice?**
  Both kernels handle validity, but Q1/Q6 never exercised the projection kernel's null path.

- **Q4: is the 85–95 s first cold Q1 after a refresh only the OS page cache?**
  Untested here; dropping caches wasn't permitted in this container.
