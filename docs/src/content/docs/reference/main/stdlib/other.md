---
title: Other Functions
---

<details>
<summary>Changelog (last updated v2.2)</summary>

v2.2: `current_database` requires parentheses

: `current_database` is now a function — [`current_database()`](#postgresql-compatibility-functions) — rather than a bare keyword.

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

  XTDB recognises a fixed set of parameter names — e.g. `'search_path'` returns `'"$user", public'`, `'server_version_num'` returns the reported PostgreSQL version.
