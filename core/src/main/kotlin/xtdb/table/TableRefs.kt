package xtdb.table

import clojure.lang.Symbol
import xtdb.api.DEFAULT_SCHEMA
import xtdb.api.TableRef

// Inverse of TableRef.schemaAndTable: reads our internal `schema/table` round-trip form (trie keys,
// block-catalog table names, leg names). Internal to core on purpose — user-facing table strings go
// through TableRef.parse, which reads the dotted SQL `schema.table` form instead.
internal fun fromSchemaAndTable(str: String): TableRef {
    val sym = Symbol.intern(str)
    return TableRef(sym.namespace ?: DEFAULT_SCHEMA, sym.name)
}
