package xtdb.table

import xtdb.util.requiringResolve

typealias SchemaName = String
typealias TableName = String

private val TABLE_REF_REGEX = Regex("([^/]+)/([^/]+)")

interface TableRef {
    val schemaName: SchemaName
    val tableName: TableName

    companion object {
        @JvmStatic
        fun parse(str: String): TableRef {
            val match = (TABLE_REF_REGEX.matchEntire(str) ?: error("Invalid table-ref: `$str`")).groupValues

            return tableRef(match[1], match[2])
        }
    }
}

fun tableRef(schema: SchemaName = "public", table: TableName) =
    requiringResolve("xtdb.table/->ref")(schema, table) as TableRef
