package xtdb.table

import clojure.lang.Symbol

typealias DatabaseName = String
typealias SchemaName = String
typealias TableName = String

const val DEFAULT_SCHEMA: SchemaName = "public"

data class TableRef(val schemaName: SchemaName = DEFAULT_SCHEMA, val tableName: TableName) {

    val sym: Symbol get() = Symbol.intern(schemaName, tableName)

    val schemaAndTable get() = "$schemaName/$tableName"

    companion object {
        @JvmStatic
        fun fromSchemaAndTable(str: String): TableRef {
            val sym = Symbol.intern(str)

            return TableRef(sym.namespace ?: DEFAULT_SCHEMA, sym.name)
        }
    }
}