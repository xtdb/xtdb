# Batch size at SF1 — why ~75–95 rows, and what bigger batches buy (#6285 follow-up dart)

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

### The restart catalog isn't the compacted one
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
- It looks like a recovery bug, or an unspecified behaviour. I didn't chase it (out of avenue), but it changes what every `-PnoLoad` dart measures.

**Implications for the EntityMerge/PQ dart**:
- Each merge in the bench has a 43-pointer priority queue (~5.4 compares/event) over the 6M events per scan.
- The in-process shape (2 × l01 at SF0.1) is a 2-pointer merge (~1 compare/event), so the PQ cost should drop several-fold.
- The pages it loads also shrink from 43 to ~2 per task.
- PQ work measured on `-PnoLoad` mostly measures the stale catalog.

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

ROUNDB2

## Questions
- Q-catalog: why does a restarted node's trie catalog lack compaction output that was produced after the last block?
  Is it a replica-log replay gap, or by design that compaction results only become visible at the next block?
  It blocks interpreting any `-PnoLoad` measurement — every #6285 dart, and especially the PQ dart.
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
