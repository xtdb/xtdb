---
title: Other Functions
---

<details>
<summary>Changelog (last updated v2.2)</summary>

v2.2: `version()` names XTDB

: [`version()`](#postgresql-built-in-functions) returns `'PostgreSQL 16 (XTDB 2.2.0)'`, and [`current_setting('xtdb.version')`](#postgresql-built-in-functions) returns the XTDB version on its own.

  Previously `version()` returned a bare `'PostgreSQL 16'`, indistinguishable from a real PostgreSQL.
  A client that speaks both dialects has to know which engine it has reached before it sends dialect-specific SQL, and with no intentional probe to ask, it was left keying on incidental behaviour that changes between releases.

  Upgrade: a client matching `version()` against a prefix such as `/^PostgreSQL (\d+)/` is unaffected; one matching the whole string needs to allow for the parenthetical.

v2.2: `current_database` requires parentheses

: `current_database` is now a function — [`current_database()`](#postgresql-built-in-functions) — rather than a bare keyword.

  Previously `SELECT current_database` parsed as a reference to a reserved keyword.
  Now it's a regular function call, which lets tools like Metabase use `SELECT current_database() AS current_database` (same name as keyword and column alias) without a parse error.

  Upgrade: rewrite any bare `current_database` references as `current_database()`.

</details>

`CARDINALITY(list)`
: returns the number of elements in the list.

`ARRAY_LENGTH(array, dimension)` (v2.2+)
: returns the number of elements in `array` at the given dimension.

  - XTDB arrays are 1-dimensional, so `dimension` must be `1`; any other value throws.
  - PostgreSQL-compatible.

`ARRAY_LOWER(array, dimension)` (v2.2+)
: returns the lower bound of `array` at the given dimension.

  - Always returns `1` — XTDB arrays are 1-indexed with no custom lower bounds.
  - `dimension` must be `1`; any other value throws.
  - PostgreSQL-compatible.

`LENGTH(expr)`
: returns the length of the value in `expr`, where `<expr>` is one of the following:
  - A **string**: returns the number of utf8 characters in the string (alias for `CHAR_LENGTH`)
  - A **byte-array**: returns the number of bytes in the array (alias for `OCTET_LENGTH`)
  - A **list**: returns the number of elements in the list (alias for `CARDINALITY`)
  - A **set**: returns the number of elements in the set
  - A **struct**: returns the number of **non-absent** fields in the struct

`TRIM_ARRAY(array, n)`
: returns a copy of `array` with the last `n` elements removed.

`obj->field`
: PostgreSQL-compatible JSON field access operator. Extracts a field from a struct by key (preserving the original type).
  - `field` must be a string literal (field name) or integer literal (for array index access)
  - Returns the value at the specified field/index
  - Returns NULL if the field does not exist
  - Example: `data->'age'` returns the `age` field from the `data` struct
  - Supports chaining: `data->'nested'->'inner'` accesses nested fields

`obj->>field`
: PostgreSQL-compatible JSON field access operator. Extracts a field from a struct by key as text.
  - Same as `->` but casts the result to text (string)
  - `field` must be a string literal (field name) or integer literal (for array index access)
  - Returns the value at the specified field/index as a string
  - Returns NULL if the field does not exist
  - Example: `data->>'age'` returns the `age` field from the `data` struct as text
  - Supports chaining: `data->'nested'->>'inner'` accesses nested fields and returns as text

`obj#>path`
: PostgreSQL-compatible JSON path access operator. Extracts a nested field by following a path (preserving the original type).
  - `path` must be a literal array of string/integer elements (e.g., `ARRAY['nested', 'inner']`)
  - Returns the value at the specified path
  - Returns NULL if any step in the path does not exist
  - Example: `data #> ARRAY['nested', 'inner']` accesses `data.nested.inner`
  - Equivalent to chaining `->` operators but more concise for deep paths

`obj#>>path`
: PostgreSQL-compatible JSON path access operator. Extracts a nested field by following a path as text.
  - Same as `#>` but casts the result to text (string)
  - `path` must be a literal array of string/integer elements (e.g., `ARRAY['nested', 'inner']`)
  - Returns the value at the specified path as a string
  - Returns NULL if any step in the path does not exist
  - Example: `data #>> ARRAY['nested', 'inner']` accesses `data.nested.inner` as text
  - Equivalent to chaining `->` operators and ending with `->>`

## PostgreSQL built-in functions

`current_database()` (v2.2+)
: returns the name of the current database.

`current_setting(name)` (v2.2+)
: returns the value of a GUC parameter.

  XTDB recognises a fixed set of parameter names, and throws on any other:

  - `'search_path'` returns `'public'`.
  - `'server_version_num'` returns `'160000'` — the PostgreSQL version XTDB reports compatibility with, not XTDB's own version.
  - `'xtdb.version'` returns XTDB's own version, e.g. `'2.2.0'`.

`version()`
: returns `'PostgreSQL 16 (XTDB 2.2.0)'`.

  The `PostgreSQL 16` prefix is the wire-protocol compatibility level XTDB claims; the parenthetical names the engine answering, in the position PostgreSQL fills with build information.

  Also spelled `pg_catalog.version()`.

## XTDB functions

`xt.version()`
: returns XTDB's version and build, e.g. `'XTDB @ 2.2.0 [a1b2c3d]'`.

  The bracketed value is the build's short git SHA, and is omitted where the build doesn't carry one.
  For the version on its own, use [`current_setting('xtdb.version')`](#postgresql-built-in-functions).
