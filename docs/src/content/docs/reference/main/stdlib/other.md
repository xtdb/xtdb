---
title: Other Functions
---

`LENGTH(expr)`
: returns the length of the value in `expr`, where `<expr>` is one of the following:
  - A **string**: returns the number of utf8 characters in the string (alias for `CHAR_LENGTH`)
  - A **byte-array**: returns the number of bytes in the array (alias for `OCTET_LENGTH`)
  - A **list**: returns the number of elements in the list
  - A **set**: returns the number of elements in the set
  - A **struct**: returns the number of **non-absent** fields in the struct
