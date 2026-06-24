package xtdb.table

import clojure.lang.Symbol

typealias DatabaseName = String
typealias SchemaName = String
typealias TableName = String

data class TableRef(val schemaName: SchemaName = "public", val tableName: TableName) {

    val sym: Symbol get() = Symbol.intern(schemaName, tableName)

    val schemaAndTable get() = "$schemaName/$tableName"

    companion object {
        @JvmStatic
        fun parse(str: String): TableRef {
            val sym = Symbol.intern(str)

            return TableRef(sym.namespace ?: "public", sym.name)
        }
    }
}