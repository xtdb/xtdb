package xtdb.table

import clojure.lang.Symbol

typealias DatabaseName = String
typealias SchemaName = String
typealias TableName = String

data class TableRef(val dbName: DatabaseName, val schemaName: SchemaName = "public", val tableName: TableName) {

    val sym: Symbol get() = Symbol.intern(schemaName, tableName)

    companion object {
        @JvmStatic
        fun parse(dbName: DatabaseName, str: String): TableRef {
            val sym = Symbol.intern(str)

            return TableRef(dbName, sym.namespace ?: "public", sym.name)
        }
    }
}