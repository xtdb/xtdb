# Batch size at SF1 — why ~75–95 rows, and what bigger batches buy (#6285 follow-up dart)

**TL;DR**
- **Why the batches are small:**
  - The scan emits one relation per leaf page per merge task.
  - On the bench's restarted node (#6174) each task merged 43 tiny L0/L1 page slices (~136 rows/batch).
  - Refreshed, it merges 6 pages (4 × l01 + 2 × l02) at ~244 rows/batch.
- **Bigger batches via a copy don't pay yet:**
  - Coalescing a task's pages into one batch costs as much as it saves: +6% / +2% on Q1 / Q6 refreshed, and it cancels most of the kernels' win.
  - The copy is the per-row `rowCopier`.
  - A fast typed gather-copy is the prerequisite.
- **Kernels alone stay the best variant:** Q1 −7.4%, Q6 −15.6% on the refreshed layout.

Pushed to `dart/6285-vector-kernels` and stacked on the kernels commits, not to a new `dart/6285-batch-size`: this session's config forbids pushing any other branch without the user's say-so.
The batch-only variant runs with `-Pkernels=none -PcomposeSelections=false`, which leaves only inert diffs against main.
Baseline is a separate checkout at main 418e8b4 + harness 5d57965, with `-PnarrowVarTypes`.
Toggles: `-Pcoalesce=none|pre|post`, `-PcoalesceTarget=N` (post only), `-PscanDebug` (shape histograms printed at JVM exit, lineitem scans only).

## The shape

`-PscanDebug`, SF1 node dir after the harness's load + compact, **restarted with `-PnoLoad`** (as every dart's bench does), cold + 1 hot of Q1 and Q6 (4 lineitem scans):

| | value |
|---|---|
| merge tasks per scan | 1024 (path depth 5, so every task is a depth-5 IID-path leaf) |
| pages per task | **43, every task: 38 × l00 + 5 × l01** |
| rows per loaded page (whole page, *before* the task's IID-path slice) | l00: mean 403 (all < 512); l01: mean 482 (< 1024) |
| rows per page after the IID-path slice (orchestrator's EntityMerge dart, same data) | 39 × ~99 and 4 × ~502 per task; rows = entities = events, with no entity spanning pages |
| relations emitted per task | 43, one per leaf page (`BitemporalConsumer.build` → `rels.map`) |
| rows per emitted relation | mean 136 before the predicates; after them, ~136 for Q1 (98% pass) and ~2 for Q6 |
| l02 (p0–p3) pages read | **none** |

So it's both factors, multiplied:
1. **The scan emits one relation per leaf page per merge task.**
   A task's resolved rows are split across its 43 source pages, so each gets ~1/43 of them.
2. **The pages a task merges come from a barely-compacted catalog.**
   There are 38 L0 pages + 5 L1 pages per task.
   Each loaded page is ~4× the task's slice of it (a page covers a shorter IID prefix than the depth-5 task), so a task's ~5.9K rows are spread thinly across 43 relations.

### The restart catalog isn't the compacted one (xtdb/xtdb#6174)
Same instrumentation at SF0.1 (`~/bench/sf01`, 7 × l00 + 7 × l01 on disk):

| run | pages per task |
|---|---|
| loading JVM (load + compact, then query in-process) | **2 × l01** |
| restart, `-PnoLoad` | **4 × l00 + 1 × l01** |
| second restart, `-PnoLoad` | 4 × l00 + 1 × l01 (stable) |

- By `trie-cat.allium`, an L1C supersedes L0 up to its block index, and L1H/L2 tries become live as their job groups complete.
  The in-process run reads only l01; every restart reads L0 files that compaction had superseded.
- So the restarted node's trie catalog lacks the compaction output from after the last block.
  The harness's `compact` stage runs after `finish-block`; the l02 files exist on disk (SF1 has l02 p0–p3) but are never read.
- `db.allium` says `TriesAdded` from the compactor goes source → leader → replica log and is applied by every node.
  A restart replaying from the last block should therefore recover it.
- It's xtdb/xtdb#6174: the restarted node plans from the snapshot cached at the last block until a transaction arrives.

**Implications for the EntityMerge/PQ dart**:
- Each merge in the bench has a 43-pointer priority queue (~5.4 compares/event) over the 6M events per scan.
- The in-process shape (2 × l01 at SF0.1) is a 2-pointer merge (~1 compare/event), so the PQ cost should drop several-fold.
- The pages it loads also shrink from 43 to ~2 per task.
- PQ work measured on `-PnoLoad` mostly measures the stale catalog.
- On the refreshed layout it's a 6-way merge (4 × l01 + 2 × l02), and event merge falls to ~17% of scan samples (JFR above).

### Refreshed layout (#6174 workaround)
The bench now runs a `:refresh-snapshot` stage after start under `-PnoLoad`: one `put-docs` into `bench_refresh`, then sync.
`-PstaleSnapshot` opts out.
I added it to both the base checkout and the variant checkout.
Shape with it (`-PscanDebug`, cold + 3 hot runs of Q1 + Q6):

| | stale (restart, no refresh) | refreshed |
|---|---|---|
| merge tasks per scan | 1024 (depth 5) | **4096 (depth 6)** |
| pages per task | 43 (38 × l00 + 5 × l01) | **6 (4 × l01 + 2 × l02-pN)** |
| rows per loaded page (pre-slice) | l00 403, l01 482 | l01 459, l02 502 |
| relations emitted per task | 43 | 6 |
| rows per emitted relation, pre-predicate | 136 | **244** (88K < 128, 42K < 256, 43K < 512, 22K < 1024) |
| rows per task | ~5.9K | ~1.47K |
| hot Q1 / Q6, one JVM, base-equivalent | ~8.0 / 6.05 s | ~7.3 / 5.2 s |

- On the realistic layout batches are still small (~244 rows), because emission is still one relation per page.
  But the merge is 6-way, not 43-way.
- Cold Q1 on the first refreshed run took 85 s (first read of the l01/l02 files in this node dir); later JVMs aren't affected.

## Routes to bigger batches

- **pre** — `concatRels`: copy a merge task's per-page relations (post-resolution, pre-predicate) into one owned `Relation` via `RelationReader.rowCopier(...).copyRange`.
  The predicates and everything downstream then see one ~5.8K-row batch per task.
  It copies all ~6M live rows × requested cols once per scan.
- **post** — run the predicates per page as today, then append the survivors into an owned `Relation`, emitting at ≥ `coalesceTarget` (1024) rows across pages *and* tasks, and flushing the remainder at the end.
  It copies only survivors.

Neither concatenates virtually: #4968's `ConcatVector` lesson (a per-row child lookup) argued for a contiguous copy.

### Shape after coalescing (`-PscanDebug`, cold + 2 hot runs of Q1 + Q6)
- pre: 8192 batches into the predicates, mean 2944 rows; Q1's batches are ~5.8K rows (<8192 bucket), Q6's ~120 after predicates.
- post: 21032 batches out, mean 1146 rows (target 1024); the predicates still run on 352K batches of ~136 rows.

### Smoke (one JVM, 3 reps, drop 1)
- pre: Q1 7754, Q6 5537 ms.
- post: Q1 8852, Q6 6200 ms — no better than base.
  Its copy happens after per-page predicates, which still pay their per-batch cost, and Q1 copies 98% of its rows; projection and group-by alone don't recoup it.

## Numbers

### Stale layout (no refresh), one JVM each (the round was stopped after one triple when #6174 surfaced), 50 reps, drop 20
```
base-1.log           q1   cold  11976 n  30 median    8030 mean    8145 min   7627 sd    380
base-1.log           q6   cold   6564 n  30 median    6052 mean    6083 min   5637 sd    322
pre-1.log            q1   cold  10235 n  30 median    7942 mean    7843 min   7137 sd    479
pre-1.log            q6   cold   5950 n  30 median    5618 mean    5611 min   4998 sd    366
prek-1.log           q1   cold   9806 n  30 median    7614 mean    7670 min   6894 sd    516
prek-1.log           q6   cold   5133 n  30 median    5280 mean    5337 min   4752 sd    334
```
- pre: Q1 −1%, Q6 −7%; pre + kernels: Q1 −5%, Q6 −13% against base, single JVMs.

### Refreshed layout — 3 JVMs per variant, interleaved base / pre / kern / prek, 40 reps, drop 20
```
base-1.log           q1   cold  12043 n  20 median    7948 mean    8018 min   7333 sd    357
base-1.log           q6   cold   5731 n  20 median    5150 mean    5188 min   4852 sd    252
base-2.log           q1   cold   9470 n  20 median    7548 mean    7643 min   7274 sd    271
base-2.log           q6   cold   5722 n  20 median    5036 mean    5146 min   4843 sd    252
base-3.log           q1   cold  10549 n  20 median    7519 mean    7556 min   7211 sd    233
base-3.log           q6   cold   5376 n  20 median    5112 mean    5113 min   4897 sd    142
pre-1.log            q1   cold  11400 n  20 median    8009 mean    8093 min   7741 sd    321
pre-1.log            q6   cold   6716 n  20 median    5204 mean    5250 min   5015 sd    171
pre-2.log            q1   cold  11812 n  20 median    8000 mean    8021 min   7495 sd    278
pre-2.log            q6   cold   6158 n  20 median    5254 mean    5244 min   4940 sd    159
pre-3.log            q1   cold  10260 n  20 median    7616 mean    7665 min   7414 sd    168
pre-3.log            q6   cold   5805 n  20 median    5218 mean    5208 min   4967 sd    120
kern-1.log           q1   cold   9762 n  20 median    7046 mean    7060 min   6658 sd    199
kern-1.log           q6   cold   5440 n  20 median    4300 mean    4311 min   4016 sd    170
kern-2.log           q1   cold   9691 n  20 median    6704 mean    6752 min   6343 sd    213
kern-2.log           q6   cold   4814 n  20 median    4312 mean    4316 min   4115 sd    132
kern-3.log           q1   cold   9132 n  20 median    6986 mean    7029 min   6824 sd    155
kern-3.log           q6   cold   4625 n  20 median    4376 mean    4360 min   4140 sd    126
prek-1.log           q1   cold  10452 n  20 median    7896 mean    7909 min   7515 sd    249
prek-1.log           q6   cold   5894 n  20 median    4928 mean    4960 min   4749 sd    144
prek-2.log           q1   cold  10353 n  20 median    7422 mean    7470 min   7117 sd    266
prek-2.log           q6   cold   5386 n  20 median    4710 mean    4740 min   4534 sd    137
prek-3.log           q1   cold  10074 n  20 median    7498 mean    7588 min   7271 sd    243
prek-3.log           q6   cold   5413 n  20 median    4862 mean    4916 min   4678 sd    210
```
- Q1 pre: 7548 → 8000 ms (+6.0%); per-JVM base [7948, 7548, 7519] vs pre [8009, 8000, 7616]
- Q1 kern: 7548 → 6986 ms (-7.4%); per-JVM base [7948, 7548, 7519] vs kern [7046, 6704, 6986]
- Q1 prek: 7548 → 7498 ms (-0.7%); per-JVM base [7948, 7548, 7519] vs prek [7896, 7422, 7498]
- Q6 pre: 5112 → 5218 ms (+2.1%); per-JVM base [5150, 5036, 5112] vs pre [5204, 5254, 5218]
- Q6 kern: 5112 → 4312 ms (-15.6%); per-JVM base [5150, 5036, 5112] vs kern [4300, 4312, 4376]
- Q6 prek: 5112 → 4862 ms (-4.9%); per-JVM base [5150, 5036, 5112] vs prek [4928, 4710, 4862]

- **Coalescing per task makes it slower on the realistic layout** (+6% Q1, +2% Q6), **and eats most of the kernels' win** (pre + kernels: −0.7% / −4.9%, vs kernels alone −7.4% / −15.6%).
- Kernels without coalescing are the best variant on the refreshed layout, with a gain much like the stale layout's (−6% / −16% there).
- The prediction in the kernels report — that bigger batches would multiply the kernels' win — **doesn't hold** while bigger batches cost a copy.

### JFR, refreshed layout, one JVM each, 12 reps (`analyse_stream.py`, samples under `ScanCursor.tryAdvance`)
| phase | base | pre |
|---|---|---|
| total | 7538 | 7521 |
| group-by | 2370 (31%) | 2491 (33%) |
| event merge | 1273 (17%) | 1491 (20%) |
| codegen evaluation | 1299 (17%) | 695 (9%) |
| **select/indirection compose** | **954 (12.7%)** | 37 |
| per-batch param-types/zone | 487 | 207 |
| mask → int[] | 247 | 160 |
| **coalesce copy** | 41 | **1632 (21.7%)** |
| page load | 545 | 614 |

- The copy (`FixedWidthVector.rowCopier.copyRows`: one `ArrowBuf.setBytes` per row per column, through the `IndirectVector` sel translation) costs as much as the bigger batches save: compose −917, evaluation −604, param setup −280, mask −87.
- On the refreshed layout the base spends **12.7% in `VectorIndirection.get`/selection composition**: the generated code reads every column through an `IndirectVector`, and each predicate re-selects every column.
  That is #6285's original target.
  The kernels avoid it (one `int[]`, resolved once per batch); coalescing avoids it by paying for a copy instead.

### Cold Q1 85 s on the first refreshed run
- One-off: later JVMs' cold Q1 was 9–12 s.
- The refreshed plan reads l01/l02 files that this box had never read since they were written (the stale runs read l00 + a few l01), so it's the OS page cache filling from disk on first touch.
  It isn't xtdb's memory cache, which is per-JVM and would refill on every JVM.
- I didn't verify this by dropping caches (no permission to try in this container).


## Suggested sub-goals
1. **advance (harness/bench)** — the `:refresh-snapshot` stage for `-PnoLoad`, already in the shared harness.
   Every `-PnoLoad` number before it measured the stale 43-way layout.
2. **tidy** — the scan-shape instrumentation (`-PscanDebug`: pages per task by level, rows per page, rows per emitted batch) as a permanent debug metric/cursor attribute.
   It's what made the stale catalog visible.
3. **advance** — a fast gather-copy for fixed-width vectors: `copyRows(sel)` as one typed loop over `int[]` (8-byte loads/stores), not an `ArrowBuf.setBytes` per row.
   That's the prerequisite for any coalescing to pay, and it's useful for every `rowCopier` user (sort, join build, group-by).
   It depends on nothing.
4. **advance** — re-try per-task coalescing (`pre`) on top of 3, and on top of the kernels.
   Coalescing pays only if the copy costs less than the ~2K samples of per-batch overhead it removes.
   Depends on 3.
5. **not now** — `post` coalescing (accumulate survivors).
   It loses even on the stale layout.

## Questions
- ~~Q-catalog~~: answered by the orchestrator — it's xtdb/xtdb#6174 (a restarted node plans from the snapshot cached at the last block until a tx arrives).
  The refresh stage is the workaround.
- Q-target: should coalescing live in the scan (as here) or in a general "re-batch" operator the planner can insert?
  The scan knows when a merge task's pages are done; a generic operator would also help joins and unnest emitting tiny batches.

## Assumptions, risks, invariants touched
- **Ownership**: `concatRels` returns an owned `Relation`.
  The predicates produce views over it, `openSlice` takes ref-counted slices, then the concat relation closes in a `finally`.
  `post` hands owned `Relation`s to `bufferedRels`, which the consumer `.use{}`s, and `close()` closes any pending one.
  No leak was reported in the smoke runs; there's no explicit leak test.
- **Type drift across pages**: `rowCopier` creates dest vectors from the first page's types.
  A later page with a wider type (nullable, a different union leg, a missing column → `NullVector`) depends on `vectorFor`'s promote path, and `copyRange` into an already-created vector of another type may throw.
  Lineitem is mono-typed, so this wasn't exercised.
- **Ordering**: the rows keep task-then-page order.
  Within a task they're no longer interleaved by IID across pages.
  Nothing downstream relied on that, per the tests below (`post` merges across tasks, too).
- **Memory**: `pre` holds a whole task's resolved rows (~5.8K × cols) at once, on top of the loaded pages.
  `post` holds ≤ target + one page.

## Dead-ends
- **post-coalescing alone**: the per-batch predicate cost stays, and the copy is paid on ~all of Q1's rows.
  It would make more sense combined with per-batch-cheap predicates (kernels), or for selective queries feeding expensive downstream operators.
