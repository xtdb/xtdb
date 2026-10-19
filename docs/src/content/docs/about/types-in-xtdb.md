---
title: Types in XTDB
---

You don't have to declare a column's type before you write to XTDB - but every column still has one.
XTDB works it out from the data you write, and widens it as different data arrives.

So what happens when one row's `reading` is an integer and the next one's is a string?
Or when a row doesn't have a `reading` at all?
And how does any of this end up in Apache Arrow, the format XTDB stores and transmits its data in?

We'll start off with a little type theory - if you just want to know how types work in XTDB, you can skip ahead to [How XTDB does it](#how-xtdb-does-it).

## Types are sets of values

A type is a set of values - that's it.

- `BOOLEAN` is the set `{true, false}`.
- `BIGINT` is the set of 64-bit signed integers - finite, but large.
- `VARCHAR` is the set of all strings - for practical purposes, infinite.

A value 'has' a type when it's a member of that type's set.
A value can belong to many sets at once: `42` is a 64-bit integer, it's a number, and it's 'some value or other'.

This gives us a way to compare types, too.
One type is a 'subtype' of another when its set is a subset of the other's - every circle is a shape, so `Circle` is a subtype of `Shape`.

## Type lattices

Order types by 'is a subset of', and you get a hierarchy.
In most typed languages, that hierarchy is a 'lattice' - for any two types, there are two more types we can always find:

The join (or 'least upper bound')
: the smallest type that contains both - the most specific type that every value of either type belongs to.

The meet (or 'greatest lower bound')
: the largest type that both contain - the most general type whose values belong to both.

This is easier to see with an example - here's a small class hierarchy in Kotlin:

```kotlin
sealed interface Shape
class Circle(val radius: Double) : Shape
class Square(val side: Double) : Shape
```

```d2
direction: up

nothing: Nothing
circle: Circle
square: Square
string: String
shape_type: Shape
any: "Any"
anyq: "Any? (the top)" {
  style.bold: true
}
nullq: "Nothing? (only null)"

nothing -> circle
nothing -> square
nothing -> string
nothing -> nullq
circle -> shape_type
square -> shape_type
shape_type -> any
string -> any
any -> anyq
nullq -> anyq
```

- The join of `Circle` and `Square` is `Shape` - it's what Kotlin infers for `if (big) Circle(2.0) else Square(1.0)`.
- The join of `Circle` and `String` is `Any` - the only thing they have in common is that they're both values.
- The meet of `Shape` and `Circle` is `Circle` - every circle is a shape, so the values in both are just the circles.
- The meet of `Circle` and `Square` is `Nothing` - no value is both.

Two types sit at the very ends of the lattice:

The top type
: the type that contains every value.
  In Kotlin, this is `Any?` - every type is a subtype of it.

The bottom type
: the type that contains _no_ values - the empty set, which is a subset of every set.
  In Kotlin, this is `Nothing`.
  It's the type of an expression that never produces a value, like `throw` or `TODO()` - which is why `val name: String = TODO()` type-checks: `Nothing` is a subtype of `String`.

And null?
In Kotlin, `null` belongs to a type of its own.
`String` doesn't contain `null`; `String?` is `String` with `null` added - the union of `String` and `{null}`.
`Nothing?` is the type whose only value is `null`.

Java is a little looser.
Its top is (more or less) `Object`, and `null` is a value of every reference type - a 'bottom value' that inhabits every type, which is why any reference might turn out to be `null` at runtime.[^billion]

[^billion]: Tony Hoare famously called the null reference his ['billion-dollar mistake'](https://www.infoq.com/presentations/Null-References-The-Billion-Dollar-Mistake-Tony-Hoare/) - I suspect he was being conservative.

## How XTDB does it

XTDB's types differ from Kotlin's in one key respect: there's no class hierarchy.
Its basic types - 64-bit integers, strings, timestamps and so on - don't overlap: no value is both an integer and a string.
That holds even between neighbouring numeric types - a value written as a 32-bit integer stays a 32-bit integer, rather than also counting as a 64-bit one.

So what's the join of an integer and a string, if there's no `Shape` or `Any` for it to land on?

XTDB takes the join literally.
If a type is a set of values, the smallest set containing all the integers and all the strings is the _union_ of the two - so XTDB's join of `BIGINT` and `VARCHAR` is 'a `BIGINT` or a `VARCHAR`'.

### Notation

XTDB shows you its own spelling of a column's type in the `xt_type` column of `information_schema.columns`, alongside the Postgres-compatible `data_type`.
We'll use that spelling for the rest of this page:

| `xt_type` | Meaning |
| --- | --- |
| `:i64`, `:utf8`, `:f64`, `:instant`, … | a single basic type - `BIGINT`, `VARCHAR`, `DOUBLE PRECISION`, `TIMESTAMP WITH TIME ZONE`, … |
| `:null` | the type whose only value is `NULL` |
| `[:? :utf8]` | a nullable type - a string, or `NULL` |
| `#{:i64 :utf8}` | a union - an integer, or a string |
| `#{:i64 :utf8 :null}` | a nullable union |
| `[:struct {"name" :utf8, "age" :i64}]` | a struct/object with those keys |
| `[:list :i64]`, `[:set :utf8]` | a list or a set, with the given element type |
| `:nothing` | the bottom type - no values at all |

See [data types](/reference/main/data-types) for the full list of basic types.

### Where you'll meet the join

**Schema evolution.**
A column's type is the join of the types of every value ever written to it, so it only ever widens:

```sql
INSERT INTO readings (_id, reading) VALUES (1, 21);
INSERT INTO readings (_id, reading) VALUES (2, 'sensor offline');
INSERT INTO readings (_id) VALUES (3);

SELECT column_name, data_type, xt_type
FROM information_schema.columns
WHERE table_name = 'readings' AND column_name = 'reading';

-- column_name | data_type | xt_type
-- reading     | json      | #{:i64 :utf8 :null}
```

The third row doesn't have a `reading` at all, so the column's type picks up `:null` too - in XTDB, a missing value and a `NULL` are one and the same.
Postgres has no equivalent of a union, so `data_type` falls back to `json`.

**Queries.**
Wherever a query brings values of different types together, the result's type is their join - the branches of a `CASE`, the arguments of `COALESCE`, the two sides of a `UNION`, the elements of an array literal.
`CASE WHEN ... THEN 1 ELSE 'one' END`, for example, has type `#{:i64 :utf8}`.

**The join doesn't convert numbers.**
`:i64` and `:f64` are different sets of values, so a column holding the integer `1` and the double `1.5` has type `#{:i64 :f64}` - XTDB keeps each value exactly as you wrote it, rather than quietly turning the `1` into `1.0`.

Arithmetic is a different matter - adding an `:i64` to an `:f64` gives you an `:f64`, as you'd expect - but that's a rule of the `+` function, not of the type lattice.
Postgres clients see a widened type, too, because Postgres doesn't have unions: over the Postgres wire protocol, a column of `#{:i32 :i64}` is a `bigint`, and one of `#{:f32 :f64}` a `double precision`.

### Null

`:null` is a type in its own right - the set containing just `NULL`.
A nullable type is the join of a type with `:null`: `[:? :utf8]` is `:utf8` joined with `:null`, and `#{:i64 :utf8 :null}` is a union that may also be `NULL`.

Because the join is a union of sets, adding `NULL` twice is the same as adding it once - there's no 'nullable nullable string'.

### The bottom type: `:nothing`

`:nothing` is the empty set - the type of a column we know about but don't yet have any values for.
`CREATE TABLE users (_id, name, email)`, for example, declares three columns of type `:nothing`.

Why not just `:null`?
The difference shows up on the first write.
The join of `:nothing` with any type is just that type, so after inserting a string into `name`, its type is `:utf8`.
Had `name` started out as `:null`, it'd now be `[:? :utf8]` - nullable for evermore, because of a `NULL` that nobody ever wrote.

### The top type

Strictly speaking, XTDB has a top type too - the union of every other type.
We never name it, though: it'd have to include every possible struct shape, so it's infinite, and a column 'of any type at all' doesn't tell a query anything it can use.

Similarly, XTDB doesn't have any need for the meet - it only ever has to find a type that _covers_ two others, never the values they have in common.

### Composite types

The join looks inside structs, lists and sets, rather than stopping at their outer shape.

Structs join key by key.
A key that only one side has becomes nullable - again, a missing key reads as `NULL`:

```
[:struct {"a" :i64}]  ⊔  [:struct {"a" :utf8, "b" :bool}]
  =  [:struct {"a" #{:i64 :utf8}, "b" [:? :bool]}]
```

Lists join element-wise, as do sets:

```
[:list :i64]  ⊔  [:list :utf8]  =  [:list #{:i64 :utf8}]
```

The result is one list type whose elements may be either - not a union of two different list types.

A list and a set don't merge, though - they're different kinds of collection, so their join is a union:

```
[:list :i64]  ⊔  [:set :i64]  =  #{[:list :i64] [:set :i64]}
```

### Functions over unions

When you call a function on a union, XTDB compiles it once for each combination of its arguments' types, and the result's type is the join of those results.
With `x` of type `#{:i64 :f64}`, `x + 1` is compiled for `:i64 + :i64` and for `:f64 + :i64`, and has type `#{:i64 :f64}`.

If any of those combinations isn't supported, the query fails when it's compiled - even if no row in your data would ever reach it.
With `x` of type `#{:i64 :utf8}`, `x + 1` fails with "`+` not applicable to types utf8 and i64".

## Types in Apache Arrow

XTDB stores its data - and returns it to Arrow-native clients (ADBC, Flight SQL) - in [Apache Arrow](https://arrow.apache.org/) format.
Arrow has its own type system, in three layers:

`ArrowType`
: the type of the values - `Int(64, signed)`, `Utf8`, `Timestamp(MICROSECOND, "UTC")`, `Struct`, `List`, `Union`, `Null` et al.
  Arrow also supports 'extension types', built on top of the core ones - XTDB uses these for its own types: `uuid`, `keyword`, `uri`, `set`, `tstz-range` and friends.

`FieldType`
: an `ArrowType`, plus whether the field is nullable.

`Field`
: a name, a `FieldType`, and child `Field`s - the keys of a struct, the elements of a list, the members of a union.

XTDB's types map onto Arrow fields as follows:

| `xt_type` | Arrow field |
| --- | --- |
| `:i64` | non-nullable `Int(64, signed)` |
| `[:? :utf8]` | nullable `Utf8` |
| `[:struct {"a" :i64}]` | `Struct`, with a child field `a` |
| `[:list :i64]` | `List`, with a single child field for its elements |
| `#{:i64 :utf8}` | non-nullable dense `Union`, with child fields `i64` and `utf8` |
| `#{:i64 :utf8 :null}` | non-nullable dense `Union`, with child fields `i64`, `utf8` and `null` |

It's not quite a perfect fit, though - the mapping bends in three places.

**A union's nullability lives in a leg.**
Arrow unions can't themselves be nullable - they don't have a validity bitmap of their own - so a nullable union carries an extra `null`-typed child, and a `NULL` value is written to that child.

**Union members are named by their Arrow type.**
Each child of a union is named after its type: `i64`, `utf8`, `struct`, `list`, `set`, `timestamp-tz-micro-utc`.
So a union has at most one member per kind of collection - every struct shape merges into its one `struct` member, key by key, as above, and every list into its one `list` member.
Timestamps in different time zones are different types, and so are different members.

**A single type plus `NULL` isn't a union.**
`#{:utf8 :null}` is the same type as `[:? :utf8]`, so it's written as a nullable `Utf8` field rather than as a union with two members.
