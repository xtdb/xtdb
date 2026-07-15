package xtdb.api

import clojure.lang.Symbol
import xtdb.api.error.Incorrect

typealias DatabaseName = String
typealias SchemaName = String
typealias TableName = String

const val DEFAULT_SCHEMA: SchemaName = "public"

data class TableRef(val schemaName: SchemaName = DEFAULT_SCHEMA, val tableName: TableName) {

    val sym: Symbol get() = Symbol.intern(schemaName, tableName)

    val schemaAndTable get() = "$schemaName/$tableName"

    companion object {
        // user-facing dotted `schema.table`, schema optional — neither part may be empty or contain `.` or `/`.
        private val PARSE_REGEX = Regex("""(?:([^./]+)\.)?([^./]+)""")

        @JvmStatic
        fun parse(str: String): TableRef {
            val (schema, table) = PARSE_REGEX.matchEntire(str)?.destructured
                ?: throw Incorrect(
                    "Invalid table reference: '$str'",
                    errorCode = "xtdb.table/invalid-table-ref",
                    data = mapOf("table-ref" to str)
                )

            return TableRef(schema.ifEmpty { DEFAULT_SCHEMA }, table)
        }
    }
}