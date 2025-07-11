package xtdb.table

import clojure.lang.Symbol

typealias SchemaName = String
typealias TableName = String

data class TableRef(val schemaName: SchemaName = "public", val tableName: TableName) {

    val sym: Symbol get() = Symbol.intern(schemaName, tableName)

    companion object {
        @JvmStatic
        fun parse(str: String): TableRef {
            val sym = Symbol.intern(str)

            return TableRef(sym.namespace ?: "public", sym.name)
        }
    }
}