# XTDB's type system: the implementation

XTDB's type lattice is represented by `VectorType`, joined by `MergeTypes`, and mapped onto Arrow fields with two deliberate bends.

- **`VectorType` is the one runtime representation of a type.**
  The Clojure spellings (`:i64`, `[:? :utf8]`, `#{…}`) are a rendering of it.

- **Every type is a set of `Mono` legs, and the join merges those sets.**

- **`mergeTypes` is the lattice join; `LeastUpperBound` and `col-type-hierarchy` are different orders.**

- **A function over unions compiles once per combination of its arguments' legs.**

- **The Arrow mapping bends where Arrow has no equivalent.**
  A union's nullability is a `null` leg, and `Nothing` is a non-nullable `Null` field — a hack.

Pre-reading: [Types in XTDB](../../docs/src/content/docs/about/types-in-xtdb.md), which covers types-as-sets, the join, `:null`, `:nothing`, unions, and the Arrow type layers.
Code references are by name; Kotlin paths are under `api/src/main/kotlin/xtdb/` or `core/src/main/kotlin/xtdb/`.

## `VectorType`

- **`VectorType` (`arrow/VectorType.kt`) is a sealed hierarchy of four shapes.**

  ```
  VectorType
  ├── Mono                       a single kind of value; may be a union leg
  │   ├── Null                   the type of NULL                              :null
  │   ├── Scalar(arrowType)      a non-null, non-union Arrow type              :i64, :utf8, :uuid, …
  │   ├── Listy(arrowType, el)   list, set, map, fixed-size list, tstz-range   [:list :i64], [:set …]
  │   └── Struct(children)       keys to VectorTypes                           [:struct {"a" :i64}]
  ├── Maybe(mono)                mono ∪ null                                   [:? :utf8]
  ├── Poly(Set<Mono>)            a union; Null may be a member                 #{:i64 :utf8 :null}
  └── Nothing                    the bottom: no values                         :nothing
  ```

  - **Extension types are `Scalar`s, except `SetType` and `TsTzRangeType`, which are `Listy`.**
    The extensions live in `arrow/extensions/`: `KeywordType`, `UuidType`, `UriType`, `TransitType`, `OidType`, `RegClassType`, `RegProcType`, `IntervalMDMType`, `SetType`, `TsTzRangeType`.

  - **A map is a `Listy` of key/value structs.**
    `mapTypeOf(k, v)` builds `Listy(Map, Struct{key, value})`.

  - **`Struct` and `Listy` children are full `VectorType`s.**
    A struct key or a list's elements can be `Maybe`, `Poly` or `Nothing`.

- **Every type is a set of `Mono` legs.**
  `VectorType.legs` makes the types-as-sets reading literal:

  | Type | `legs` |
  | --- | --- |
  | `Nothing` | `{}` |
  | `Null` | `{Null}` |
  | any other `Mono` `m` | `{m}` |
  | `Maybe(m)` | `{m, Null}` |
  | `Poly(s)` | `s` |

  - **`fromLegs(Set<Mono>)` normalises a set of legs to its one canonical type.**
    No non-null legs → `Null`.
    One non-null leg → that leg, in a `Maybe` if `Null` was present.
    Several → `Poly`.

  - **`maybe(type)` adds `Null` to a type by the same rules.**
    `Nothing` becomes `Null`, a `Mono` a `Maybe`, a `Poly` gains a `Null` leg; `Null` and `Maybe` are unchanged.

  - **The shape invariants hold through the factories, not the types.**
    A `Maybe` wraps only a `Mono`, and a `Poly` has at least two non-null legs — but the constructors are public, so only `fromLegs` and `maybe` guarantee either.
    Build types through them.

  - **`fromLegs(∅)` answers `Null`, not `Nothing`.**
    `fromLegs()` with no arguments is the marker for an unknown prepared-statement parameter (`Xtdb.kt`, `TxWriter.kt`, `pgwire.clj`).
    The join reaches the bottom through `MergeTypes`, never through `fromLegs(∅)`.

## The join: `MergeTypes`

- **`MergeTypes.mergeTypes` (`arrow/MergeTypes.kt`) is the lattice join, and it is total.**
  Clojure calls it as `xtdb.types/merge-types`.
  It flattens each input to its legs, merges them into an accumulator, and answers the accumulator's type.

  - **Legs of the same kind merge; legs of different kinds stay separate legs.**
    Which rule applies is picked by the leg's kind:

    - **`Null` sets the accumulator's nullable flag.**

    - **`Nothing` contributes nothing.**
      It has no legs.

    - **`Scalar`s are kept as a set, with no promotion.**
      `I32 ⊔ I64 = Poly{I32, I64}`.

    - **`Listy`s merge their element types, one leg per listy Arrow type.**
      `[:list :i64] ⊔ [:list :utf8] = [:list #{:i64 :utf8}]`, but a list and a set stay two legs.

    - **Every `Struct` merges into one struct leg, key by key.**
      A key some struct lacks is marked nullable — both a key first seen after other structs, and a key a later struct doesn't have.

  - **An accumulator that saw no legs answers `Nothing`.**
    So `Nothing ⊔ Nothing = Nothing`, nested too: an empty list literal is `[:list :nothing]` (#5871).

  - **The Arrow mapping imposes the same leg-per-kind rule.**
    A dense union's children are named by `ArrowType.toLeg()`, and every struct's leg name is `struct`, every list's `list`.

- **`mergeTypes` rejects a Kotlin `null` rather than skipping it.**
  A source with nothing to say contributes `Nothing`; a `null` faults with `xtdb/merge-types-null`.
  Skipping one silently drops a contribution from the join, which is how #5855 — a column's historical and live types not being joined — stayed hidden.

- **The join runs wherever types from more than one source meet.**

  - **Column types across the index.**
    `TableCatalog.mergeVecTypes` folds a finished block's types into the catalog; `Snapshot.columnTypes` joins the catalog with the live and superseded live indices.
    A source that doesn't record a column contributes `VectorType.absentContribution(recorded)`: `Null` if it has put rows (they read the column as null), `Nothing` if it has none.

  - **Expression return types** (`expression.clj`).
    `:if`, `CASE`, `COALESCE`, list and set literals, and every function call over unions.

  - **Operator output types.**
    Set operators, the nullable side of an outer join (`types/with-nullable-types`, i.e. `maybe`), `apply`, `unnest`, `table`.

- **`MergeFields` is a second implementation of the join, and it has drifted from the first.**
  `MergeFields.mergeFields` (`arrow/MergeFields.kt`) joins Arrow `Field`s rather than `VectorType`s, for compaction (`SegmentMerge`).

  - **It doesn't support maps.**
    Its `Map` visitor is a `TODO`, where `MergeTypes` merges a map like any other `Listy`.

  - **It filters out Kotlin `null`s**, where `mergeTypes` rejects them.

## Orders that aren't the lattice

- **`LeastUpperBound` is numeric and temporal promotion, not the lattice join.**
  `LeastUpperBound.of(types): Mono?` (`types/LeastUpperBound.kt`; `types/least-upper-bound` from Clojure) is the least upper bound in a different order — 'losslessly promotes to' — and that order is partial.
  Arithmetic, comparisons and aggregates use it to choose the type they compute in.

  - **It widens within numeric and within temporal types.**
    Signed ints to the wider; ints to floats or decimals; decimals to floats, or to a precision and scale that fit both; durations, times and timestamps to the finer unit, timestamps only local-with-local or zoned-with-zoned, with differing zones going to `Z`.

  - **It answers `null` where there's no common scalar** — `I64` with `Utf8`, unsigned ints, anything with a struct.

  - **It ignores `Null`, so its result is never nullable.**
    The caller re-applies nullability.

- **`col-type-hierarchy` is a dispatch classification, and its `:any` is not a top type.**
  `types/col-type-hierarchy` is a Clojure `derive` hierarchy over type heads — `:i64` under `:int` under `:num` under `:any` — and `codegen-call` dispatches on it.
  None of `:any`, `:num` or `:int` is a `VectorType`.

  - **`types/widening-hierarchy` is deprecated.**
    It extended the dispatch hierarchy with promotion edges; `LeastUpperBound` replaces it.

## Unions in the expression engine

- **A call compiles once per combination of its arguments' legs, and its type is the join of theirs** (`codegen-call*`, `expression.clj`).

  1. The cartesian product of each argument's `.getLegs` gives the combinations.
  2. Where the function short-circuits on null, a combination with a `Null` leg returns `:null` without a call.
  3. `codegen-call` runs once for each remaining combination, with monomorphic `:arg-types`.
  4. The call's return type is `merge-types` over every combination's return type.
  5. At runtime, `continue-read-union` dispatches on the reader's `.getLeg`, after an `.isNull` check if `Null` is a leg.

  - **A combination with no `codegen-call` method fails the query at compile time.**
    The `::default` method throws `function-type-mismatch`, whether or not any row would reach that combination.

  - **Reading a `Nothing` is a defined fault** (`::zero-leg-read`, #5948).
    The bottom has no cells to read.

## Arrow mapping

- **`toField(name)` maps each shape to one Arrow field.**

  | `VectorType` | `Field` |
  | --- | --- |
  | `Mono` | `FieldType(nullable = (it's Null), arrowType)`, children from `children` |
  | `Maybe(m)` | `m`'s field, nullable |
  | `Poly` with one non-null leg | that leg's field, nullable |
  | `Poly` | non-nullable dense union of `asLegField`s, named by `ArrowType.toLeg()`, with a `null` leg if nullable |
  | `Nothing` | non-nullable `Null` |

- **`Field.asType` reads a field back, folding the field's nullability in through `maybe`.**

  - **A dense union reads through `fromLegs` of its children.**

  - **A run-end-encoded field reads as its values child.**
    REE is a layout, not a type, so a run-encoded `I64` column joins a plain one without becoming a union.

- **The mapping bends in three places.**

  - **Arrow unions can't be nullable.**
    `DenseUnionVector` refuses to be set nullable; a nullable union has a `null` leg instead.

  - **Leg names are derived, not round-tripped.**
    `toLeg()` lowercases a timestamp's zone and replaces `/` and `:` — `timestamp-tz-micro-europe_london` — so distinct zones are distinct legs.

  - **Sparse unions and the `Large*` and `*View` Arrow types are unsupported.**

- **`Nothing` is a non-nullable `NullVector` — a hack.**
  Arrow has no bottom type, so XTDB encodes `Nothing` as a `Null`-typed field marked non-nullable, a combination with no other use.

  - **The encoding runs through fields, vectors and the wire.**
    `Nothing.toField` writes `FieldType(false, NULL_TYPE)`, and `Field.asType` reads a non-nullable `NULL_TYPE` back as `Nothing`.
    `NullVector(nullable = false)` holds only undefined cells (`writeUndefined`), and its first `writeNull()` turns it into `Null` for good.
    On pgwire, `Null` and `Nothing` share `PgType.Null`.

  - **A type-level fact rides on a vector-level flag, so normalising the flag loses the bottom.**
    Rebuild a field with the flag normalised, or read 'Arrow `Null`' as the `Null` `VectorType` → the bottom becomes `Null` → `Null ⊔ X = Maybe(X)`, and every type it's joined with turns nullable.

    - **Three sites have done this, each since fixed.**
      `fromLegs(∅)` answering `Null` for an empty merge (#5871); a column's historical and live types not being joined (#5855); `codegen-expr :variable` rewriting a whole-column `Nothing` to `:null` (`4ea837973`).

  - **Code that builds `Field`s or `NullVector`s has to carry a non-nullable `Null` through unchanged.**

  - **Ending a row takes a short column off the bottom.**
    `Relation.endRows` pads every column short of the row count with `writeNull()`; only a column given `writeUndefined()` for every row stays `Nothing`.

## Clojure notation

- **`#xt/type` (`xtdb.serde.types/->type`) reads the spec notation into a `VectorType`, and `render-type` writes it back.**
  `information_schema.columns.xt_type` is `render-type`'s output.

  | Spec | `VectorType` |
  | --- | --- |
  | `:i64`, `:utf8`, … | `Scalar` |
  | `:null` | `Null` |
  | `:nothing` | `Nothing` |
  | `[:? t]` | `Maybe` |
  | `#{t1 t2}` | `Poly` |
  | `[:struct {"k" t}]` | `Struct` |
  | `[:list t]`, `[:set t]`, `[:map {:sorted? b} t]`, `:tstz-range` | `Listy` |

  - **A nullable composite renders flattened.**
    `[:? :list :i64]`, not `[:? [:list :i64]]`; `->type` reads either.

  - **`[:fixed-size-list n t]` renders, but doesn't read.**
    `->type` takes `n` as the element type and throws, so a fixed-size list doesn't round-trip through the notation.

- **The older col-type representation is legacy.**
  `[:union #{…}]` with `:null` as a member, and `[:struct {…}]` over col-types.
  Its functions in `xtdb.types` — `merge-col-types`, `field->col-type`, `vec-type->col-type`, `arrow-type->col-type`, `widening-hierarchy` — are `^:deprecated`; new code works in `VectorType`.
