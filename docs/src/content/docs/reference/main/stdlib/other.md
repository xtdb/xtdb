---
title: Other Functions
---

`CARDINALITY(list)`
: returns the number of elements in the list.

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
