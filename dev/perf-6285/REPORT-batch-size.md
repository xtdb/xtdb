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
| rows per page | l00: mean 403 (all < 512); l01: mean 482 (< 1024) |
| rows read per scan | ~18M, to emit 6M live rows — **3× read amplification** |
| relations emitted per task | 43, one per leaf page (`BitemporalConsumer.build` → `rels.map`) |
| rows per emitted relation | mean 136 before the predicates; after them, ~136 for Q1 (98% pass) and ~2 for Q6 |
| l02 (p0–p3) pages read | **none** |

So it's both factors, multiplied:
1. **The scan emits one relation per leaf page per merge task.**
   A task's resolved rows are split across its 43 source pages, so each gets ~1/43 of them.
2. **The pages a task merges come from a barely-compacted catalog.**
   With 38 L0 pages + 5 L1 pages per task, ~2/3 of the rows read are duplicates that the merge resolves away, and the live rows spread thinly across all 43 relations.

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
- Each merge in the bench has a 43-pointer priority queue (~5.4 compares/event) over 18M events per scan.
- The in-process shape is 2–5 pointers over ~6M events: ~3× fewer events, ~2.5× shallower heap, so ~7× less PQ work.
- PQ work measured on `-PnoLoad` mostly measures the stale catalog.

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

ROUNDB1

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
