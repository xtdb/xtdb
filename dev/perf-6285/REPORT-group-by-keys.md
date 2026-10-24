# #6285 side question: group-by key hashing/equality (dart report)

The spike is the commit "dart(group-by keys): pointer utf8 equality + unchecked xxhash reads" (`PointerEquiComparator.kt`, `distinct.clj`, `Hasher.kt`).
It was benched on main + harness + refresh-snapshot (#6174), with `-PptrEqui`/`-PfastHash` toggles on one build.
I couldn't read #6163/#6167/#4971 from this session.

## 1. What we're doing (Q1)

- `GROUP BY l_returnflag, l_linestatus`: two utf8 keys of 1 char each, giving **4 groups** (A/F, N/F, N/O, R/F).
- ~5.9M input rows (98% of lineitem pass `l_shipdate <= …`), in ~244-row batches in the refreshed layout, so ~24k batches.
- Per row, the job is to map the key tuple to a group id: one of 4 ids, nearly always an existing one.
  `GroupMapper.Mapper.groupMapping` writes the ids into an `I32` vector that the aggregates read.

## 2. How we do it today (per row unless stated)

- **Per batch**: `DistinctRelationMap.buildFromRelation` (`core/src/main/kotlin/xtdb/operator/distinct/DistinctRelationMap.kt:51-62`) does the following.
  - Builds the comparator through `ComparatorFactory.buildEqui` for each key column (`distinct.clj:66-71`).
    That calls `emap/->equi-comparator` (`expression/map.clj:40-51`), which calls `build-comparator` (`map.clj:18-35`), memoised with `util/lru-memoize` **keyed on the expression map plus `{:var-types …}`**, so it hashes and compares persistent maps every batch.
    It then wraps each column in a fresh `vr/rel-reader`, and joins the columns' comparators with `andIBO`.
  - Builds an `IndexHasher` (`expression/map/IndexHasher.kt:10-33`) and `inRel.rowCopier(accRel)`.
- **Per row**, `GroupMapper.kt:39-47` calls `builder.addIfNotPresent(idx)` (`DistinctRelationMap.kt:64-85`):
  1. **Hash**: `IndexHasher.hashCode` loops over the key columns with `MurmurHasher.combineHashCode(acc, col.hashCode(idx, hasher))`.
     For each key column the call goes `IndirectVector.hashCode` → `MonoVector.hashCode` (null check) → `VariableWidthVector.hashCode0` (`VariableWidthVector.kt:91-95`) → `ExtensibleBuffer.hashCode` (`Buffers.kt:200`) → `Hasher.Xx.hash(ArrowBuf, off, len)` (`util/Hasher.kt`).
     The last step allocates a `Bytes.Buf` wrapper, and every byte is a bounds-checked `ArrowBuf.getByte` through a sealed-interface call.
     That's ~5 virtual hops per column.
  2. **Trie**: `MutableMemoryHashTrie.addIfNotPresent` (`trie/MutableMemoryHashTrie.kt:74-82`):
     - `intToByteArray(hash)` allocates a `ByteBuffer.allocate(4)`, i.e. a `HeapByteBuffer` plus a `byte[4]`.
     - Two capturing lambdas are allocated per row: the comparator closure and the `onAddition` `Runnable` (`DistinctRelationMap.kt:72-76`).
     - `Leaf.addIfNotPresent` (`:230-246`) scans the 4 entries linearly, byte-comparing each hash against the `xt/join-hash` column via `ArrowBufPointer` and `ByteFunctionHelpers.compare`.
     - It then returns `Pair(idx, null)`, and the trie wraps it in another `Pair`: two allocations, plus `Int` boxing (cached for ids under 128).
  3. **Equality**, on a hash match (~once per row): the `andIBO` lambda calls one generated `IntBinaryOperator` per key column.
     - Each one does `setPos` on both value readers, the input side through `IndirectVector`'s value reader.
     - It then calls `readBytes()` on both sides: `VariableWidthVector.getBytes` (`:63-67`) → `ExtensibleBuffer.getBytes` → `ArrowUtil.toByteBuffer` (`ArrowUtil.kt:172-173`), which is `MemorySegment.ofAddress(…).reinterpret(len).asByteBuffer()`.
       That is **2 `MemorySegment`s + 1 direct `ByteBuffer` per side**, so 6 objects per key column and **12 per row**.
     - `:null_eq` on utf8 falls through to `[:=== :any :any]` → `clojure.core/=` (`expression.clj:815-821`), then `Util.equiv` → `ByteBuffer.equals` → `BufferMismatch.mismatch`, for a 1-byte value.
- **Total: ~20 short-lived objects and ~15–20 megamorphic calls per row, to pick one of 4 ids.**

## 3. Why it's costly

From the refreshed-layout JFR, base, 12 reps of Q1 and Q6.
The group-mapping samples are Q1's; Q6 uses `GroupMapper.Null`.
Q1 ≈ 59% of scan samples by wall time, so group mapping ≈ 815 of ~3200 Q1 samples, **~26% of Q1**, ≈ 1.3s of ~5s, **≈ 220 ns/row**.

| part of group mapping | samples | share | ≈ ns/row |
|---|---|---|---|
| utf8 read → `MemorySegment`/`ByteBuffer` allocation (equality) | 321 | 39% | ~85 |
| hashing (`Hasher.Xx` / `ExtensibleBuffer.hashCode` chain) | 176 | 22% | ~48 |
| per-batch `buildFromRelation` (`lru-memoize` key hashing/equals of expression maps, `rel-reader`, `VectorType` construction) | 119 | 15% | ~8 µs/batch |
| comparator body / `setPos` / `andIBO` dispatch | 81 | 10% | ~22 |
| `ByteBuffer.equals`/`BufferMismatch`/`Util.equiv` | 54 | 7% | ~15 |
| trie leaf scan / hash compare / `Pair` | 32 | 4% | ~9 |
| `intToByteArray` | 1 | — | — |

- **The `ByteBuffer.equals` share is JIT-sensitive.** In the sparse-variant JFR it took 285 samples instead of 54, with the same vector shapes.
  This is what made group-by look +32% worse on the indirect-select branch. Pointer equality removes that sensitivity.

## 4. What we could do instead

Each entry: what it is, its expected effect, and what it touches.

- **A. Allocation-free variable-width equality. Spiked.**
  - What: compare `getPointer(idx, reusePtr)` on both sides (`ArrowBufPointer.equals` → `ByteFunctionHelpers.equal`), with the 1/−1/0 null contract.
  - Effect, measured: 3 interleaved JVMs, Q1 only, 50 reps, drop 20.
    - Off (median of 6687 across JVMs; 6428 6510 7122 per JVM) vs ptrEqui **6396 (−4.3%)** (6216 6194 6779).
    - ptrEqui + fastHash 6216 (−7.0%) (6418 6075 6156), but noisy, and the JFR shows fastHash doesn't reduce hash samples.
    - JFR off → both: group mapping **987 → 761 samples (−23%)**.
      Allocation-plus-equals 444 → 52, and **per-batch setup 140 → 25**, since the pointer path skips the memoised comparator emit.
      Trie and comparator-body samples rose (72 → 224, 29 → 107), which looks like attribution shifting.
  - Touches: done in the spike as a `distinct.clj` fast path with a Kotlin comparator.
    The landable form is a `ValueReader.readPointer` (or similar), with codegen `==`/`null_eq`/`compare` for utf8/varbinary using it.
    That would also benefit joins, `select` predicates and sort.
- **B. Hash key columns per batch, not per row, column by column.**
  - What: `IndexHasher.writeAllHashes` already exists. Run a tight loop per key column over the *direct* vector (through `sel`) into an `int[]`, combining columns in a second pass.
  - Effect, estimated: removes the ~5-hop virtual chain per column per row, most of the ~22%.
    The fastHash spike (unchecked byte reads only) changed nothing, so the cost is the dispatch chain, not the bounds checks.
  - Touches: `IndexHasher`, `DistinctRelationMap.buildFromRelation` (precompute hashes per batch), and `VectorReader` (a batch hash method that `IndirectVector` overrides, gathering through `sel`).
- **C. Resolve comparators once per cursor, not per batch.**
  - What: key the comparator on the (build type, input type) pair, cached in the `DistinctRelationMap` rather than in `lru-memoize` over expression maps.
    Rebind only the vectors per batch, e.g. a comparator object with `bind(l, r)`.
  - Effect: the ~15% per-batch slice, ~3–4% of Q1, rising with smaller batches (the stale layout's ~99-row batches).
  - Touches: `distinct.clj` `ComparatorFactory`, `emap/->equi-comparator`, `DistinctRelationMap`.
- **D. A flat, growable open-addressing table** (hash `int[]` plus group-id `int[]`, linear probing, resize at load factor), in the style of `BuildSideMap`/#6167 but insertable.
  - Effect: removes `intToByteArray`, 2 `Pair`s, 2 capturing lambdas and the leaf byte-compare per row.
    That's only ~4–10% of group mapping at 4 groups, but it grows with group count, where the trie's paging and splitting kick in.
  - Touches: `DistinctRelationMap` (and `distinct`/`set` users of `MutableMemoryHashTrie.addIfNotPresent`).
- **E. Dictionary-encoded keys** (#4971; Q1's keys are 1-char flags).
  - What: with dictionary indices, the group key becomes an int tuple (ids < 4), so hashing is an int mix, equality is `==`, and a direct-indexed `int[dictA × dictB]` → group id needs no hash table at all.
  - Effect: almost all of the ~26% of Q1. This is the biggest single lever, but also the largest change: storage format, ingest, metadata, and every reader of the column.
- **F. Specialise the 1–2 key-column case**, generating one fused `addIfNotPresent` per key-type tuple instead of `andIBO` over per-column `IntBinaryOperator`s.
  - Effect: the 10% comparator-body/dispatch slice. It's cheap once A and C exist.

Suggested order: **A** (spiked, small, broadly useful), then **C**, then **B**, then **D**; **E** is its own project.
A + B + C together plausibly halve group mapping, roughly −10–13% on Q1, without a format change.

## Caveats

- These spike runs came hours after the earlier refreshed-layout runs.
  Q1 "off" ran at 6.4–7.1s, against ~5.0s for the earlier fresh base, on the same code path.
  Each `-PnoLoad` run adds a refresh tx to the node, so the layout may have drifted, or the box is noisier; compare within this table only.
- `PointerEquiComparator` assumes both sides are mono `Utf8`/`Binary` (checked); any other type falls back to codegen.
  The comparator holds two reusable `ArrowBufPointer`s, so it isn't thread-safe; that matches the current per-batch, single-thread use.
- The fastHash `MemoryUtil` reads skip bounds checks: unsafe on a bad offset, and no measurable win here.

runs.py (Q1 only):
```
off-1.log            q1   cold  94485 n  30 median    6428 mean    6399 min   6043 sd    179
off-2.log            q1   cold  10227 n  30 median    6510 mean    6534 min   6143 sd    227
off-3.log            q1   cold  10883 n  30 median    7122 mean    7108 min   6715 sd    215
ptr-1.log            q1   cold   9729 n  30 median    6216 mean    6231 min   5940 sd    196
ptr-2.log            q1   cold  10274 n  30 median    6194 mean    6211 min   5836 sd    176
ptr-3.log            q1   cold  10759 n  30 median    6779 mean    6811 min   6363 sd    222
both-1.log           q1   cold  10464 n  30 median    6418 mean    6440 min   5963 sd    243
both-2.log           q1   cold   9799 n  30 median    6075 mean    6078 min   5705 sd    199
both-3.log           q1   cold  10236 n  30 median    6156 mean    6226 min   5976 sd    189
```
