package xtdb.mysql

import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import java.io.Serializable
import java.nio.charset.Charset

data class TableId(val database: String, val table: String) {
    override fun toString() = "$database.$table"

    companion object {
        fun parse(s: String): TableId {
            val parts = s.split('.')
            if (parts.size != 2 || parts.any { it.isBlank() })
                throw Incorrect(
                    "'$s' is not a table — name tables as 'database.table'",
                    errorCode = "xtdb.mysql/invalid-table-name",
                    data = mapOf("table" to s),
                )
            return TableId(parts[0], parts[1])
        }
    }
}

data class ColumnDef(val name: String, val type: MySqlType)

data class TableDef(val id: TableId, val columns: List<ColumnDef>) {

    fun fromBinlog(row: Array<Serializable?>): Map<String, Any?> {
        check(row.size == columns.size) { "row of ${row.size} values for $id's ${columns.size} columns" }
        return columns.indices.associate { i -> columns[i].name to row[i]?.let { columns[i].type.fromBinlog(it) } }
    }

    companion object {
        private const val BINARY_COLLATION = 63

        // ENUM and SET are logged as STRING, with their real type in the metadata's high byte
        private const val ENUM = 247
        private const val SET = 248

        /** A table as its `TABLE_MAP` event describes it, which needs `binlog_row_metadata=FULL`. */
        fun fromTableMap(data: TableMapEventData, charsetOf: (collationId: Int) -> Charset?): TableDef {
            val id = TableId(data.database, data.table)

            val meta = data.eventMetadata
            val names = meta?.columnNames
                ?: throw Incorrect(
                    "the binlog carries no column names for $id — binlog_row_metadata must be FULL",
                    errorCode = "xtdb.mysql/upstream-setting",
                    data = mapOf("table" to id.toString(), "setting" to "binlog_row_metadata"),
                )

            // charsets, ENUM values and SET values are each listed per column of their own kind, not per ordinal
            var charColumn = 0
            var enumColumn = 0
            var setColumn = 0

            fun collation(): Int {
                val ordinal = charColumn++
                meta.columnCharsets?.let { return it[ordinal] }
                val default = meta.defaultCharset
                    ?: throw Incorrect("the binlog carries no charsets for $id", errorCode = "xtdb.mysql/upstream-setting", data = mapOf("table" to id.toString(), "setting" to "binlog_row_metadata"))
                return default.charsetCollations?.get(ordinal) ?: default.defaultCharsetCollation
            }

            fun charType(code: Int, m: Int): MySqlType {
                val collation = collation()

                // STRING packs the top bits of its length into the metadata's high byte
                val fixedLength = if (code == ColumnType.STRING.code) (((m shr 4) and 0x300) xor 0x300) + (m and 0xFF) else null

                return if (collation == BINARY_COLLATION) MySqlType.Binary(fixedLength)
                else MySqlType.Text(
                    charsetOf(collation)
                        ?: throw Unsupported("unknown collation id $collation on $id", "xtdb.mysql/unsupported-charset", mapOf("table" to id.toString()))
                )
            }

            val columns = names.indices.map { i ->
                val m = data.columnMetadata[i]
                val code = (data.columnTypes[i].toInt() and 0xFF)
                    .let { if (it == ColumnType.STRING.code && (m shr 8) in listOf(ENUM, SET)) m shr 8 else it }

                val name = names[i]
                val unsigned = meta.signedness?.get(i) == true

                val type = when (code) {
                    ColumnType.TINY.code -> MySqlType.Integral(1, unsigned)
                    ColumnType.SHORT.code -> MySqlType.Integral(2, unsigned)
                    ColumnType.INT24.code -> MySqlType.Integral(3, unsigned)
                    ColumnType.LONG.code -> MySqlType.Integral(4, unsigned)
                    ColumnType.LONGLONG.code -> MySqlType.Integral(8, unsigned)
                    ColumnType.FLOAT.code -> MySqlType.Single
                    ColumnType.DOUBLE.code -> MySqlType.Real
                    ColumnType.NEWDECIMAL.code -> MySqlType.Decimal

                    ColumnType.VARCHAR.code, ColumnType.VAR_STRING.code, ColumnType.STRING.code, ColumnType.BLOB.code -> charType(code, m)

                    ENUM -> MySqlType.Enumeration(meta.enumStrValues[enumColumn++].toList())
                    SET -> MySqlType.Members(meta.setStrValues[setColumn++].toList())
                    ColumnType.BIT.code -> MySqlType.Bit((m shr 8) * 8 + (m and 0xFF))

                    ColumnType.DATE.code -> MySqlType.Date
                    ColumnType.DATETIME_V2.code -> MySqlType.Datetime
                    ColumnType.TIMESTAMP_V2.code -> MySqlType.Timestamp
                    ColumnType.TIME_V2.code -> MySqlType.Time
                    ColumnType.YEAR.code -> MySqlType.Year
                    ColumnType.JSON.code -> MySqlType.JsonDoc
                    ColumnType.GEOMETRY.code -> MySqlType.Binary()

                    ColumnType.DATETIME.code, ColumnType.TIMESTAMP.code, ColumnType.TIME.code -> throw legacyTemporal(id, name)

                    else -> throw Unsupported(
                        "column '$name' on $id has MySQL type code $code, which this source can't read",
                        "xtdb.mysql/unsupported-column-type",
                        mapOf("table" to id.toString(), "column" to name, "type-code" to code),
                    )
                }

                ColumnDef(name, type)
            }

            return TableDef(id, columns)
        }

        data class InfoSchemaColumn(val name: String, val dataType: String, val columnType: String, val numericPrecision: Long?, val characterSet: String?)

        fun fromInformationSchema(id: TableId, cols: List<InfoSchemaColumn>): TableDef =
            TableDef(id, cols.map { ColumnDef(it.name, it.mysqlType(id)) })

        private fun InfoSchemaColumn.mysqlType(id: TableId): MySqlType {
            val unsigned = "unsigned" in columnType

            return when (dataType.lowercase()) {
                "tinyint" -> MySqlType.Integral(1, unsigned)
                "smallint" -> MySqlType.Integral(2, unsigned)
                "mediumint" -> MySqlType.Integral(3, unsigned)
                "int" -> MySqlType.Integral(4, unsigned)
                "bigint" -> MySqlType.Integral(8, unsigned)
                "float" -> MySqlType.Single
                "double" -> MySqlType.Real
                "decimal" -> MySqlType.Decimal

                "char", "varchar", "tinytext", "text", "mediumtext", "longtext" ->
                    if (characterSet == null || characterSet == "binary") MySqlType.Binary()
                    else MySqlType.Text(MySqlCharsets.javaCharset(characterSet))

                "binary" -> MySqlType.Binary(columnType.substringAfter('(').substringBefore(')').toInt())

                "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
                "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection" -> MySqlType.Binary()

                "bit" -> MySqlType.Bit(numericPrecision!!.toInt())
                "enum" -> MySqlType.Enumeration(quotedMembers(columnType))
                "set" -> MySqlType.Members(quotedMembers(columnType))

                "date" -> MySqlType.Date
                "datetime" -> MySqlType.Datetime
                "timestamp" -> MySqlType.Timestamp
                "time" -> MySqlType.Time
                "year" -> MySqlType.Year
                "json" -> MySqlType.JsonDoc

                else -> throw Unsupported(
                    "column '$name' on $id has type '$dataType', which this source can't read",
                    "xtdb.mysql/unsupported-column-type",
                    mapOf("table" to id.toString(), "column" to name, "type" to dataType),
                )
            }
        }

        private fun legacyTemporal(id: TableId, column: String) =
            Unsupported(
                "column '$column' on $id uses a pre-5.6 temporal format — ALTER TABLE $id FORCE to rebuild it",
                "xtdb.mysql/legacy-temporal-column",
                mapOf("table" to id.toString(), "column" to column),
            )

        internal fun quotedMembers(columnType: String): List<String> =
            Regex("'((?:[^']|'')*)'").findAll(columnType.substringAfter('(')).map { it.groupValues[1].replace("''", "'") }.toList()
    }
}

internal object MySqlCharsets {
    fun javaCharset(mysqlName: String): Charset = Charset.forName(
        when (mysqlName.lowercase()) {
            "utf8mb4", "utf8mb3", "utf8" -> "UTF-8"
            // MySQL's latin1 is cp1252, not ISO-8859-1
            "latin1" -> "windows-1252"
            "ascii" -> "US-ASCII"
            "ucs2", "utf16" -> "UTF-16BE"
            "utf16le" -> "UTF-16LE"
            "utf32" -> "UTF-32BE"
            "sjis" -> "Shift_JIS"
            "cp932" -> "windows-31j"
            "ujis" -> "EUC-JP"
            "euckr" -> "EUC-KR"
            "latin2" -> "ISO-8859-2"
            "latin5" -> "ISO-8859-9"
            "latin7" -> "ISO-8859-13"
            "greek" -> "ISO-8859-7"
            "hebrew" -> "ISO-8859-8"
            "koi8r" -> "KOI8-R"
            "koi8u" -> "KOI8-U"
            "tis620" -> "TIS-620"
            "cp850" -> "IBM850"
            "cp852" -> "IBM852"
            "cp866" -> "IBM866"
            "cp1250" -> "windows-1250"
            "cp1251" -> "windows-1251"
            "cp1256" -> "windows-1256"
            "cp1257" -> "windows-1257"
            "macroman" -> "x-MacRoman"
            else -> mysqlName
        }
    )
}
