package xtdb.postgres

import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Converts pgoutput text-format column values to appropriate JVM types
 * based on the PostgreSQL type OID.
 *
 * pgoutput always sends values in text format, so we parse from String.
 */
object PgTypeCoercion {

    // Well-known PG type OIDs
    private const val BOOL_OID = 16
    private const val INT2_OID = 21
    private const val INT4_OID = 23
    private const val INT8_OID = 20
    private const val FLOAT4_OID = 700
    private const val FLOAT8_OID = 701
    private const val NUMERIC_OID = 1700
    private const val TEXT_OID = 25
    private const val VARCHAR_OID = 1043
    private const val BPCHAR_OID = 1042
    private const val NAME_OID = 19
    private const val DATE_OID = 1082
    private const val TIMESTAMP_OID = 1114
    private const val TIMESTAMPTZ_OID = 1184
    private const val JSON_OID = 114
    private const val JSONB_OID = 3802
    private const val UUID_OID = 2950

    fun coerce(text: String, typeOid: Int): Any = when (typeOid) {
        BOOL_OID -> text == "t"
        INT2_OID, INT4_OID -> text.toInt()
        INT8_OID -> text.toLong()
        FLOAT4_OID -> text.toFloat()
        FLOAT8_OID -> text.toDouble()
        NUMERIC_OID -> text.toBigDecimal()
        DATE_OID -> LocalDate.parse(text)
        TIMESTAMP_OID -> LocalDateTime.parse(text.replace(' ', 'T'))
        TIMESTAMPTZ_OID -> parseTimestamptz(text)
        UUID_OID -> java.util.UUID.fromString(text)
        // Text-like types (TEXT, VARCHAR, BPCHAR, NAME, JSON, JSONB) stay as String
        else -> text
    }

    private fun parseTimestamptz(text: String): Instant {
        // PG sends timestamptz as e.g. "2024-01-01 00:00:00+00" or "2024-01-01 12:30:00.123456+02"
        // Normalize to ISO-8601 for Instant.parse
        val normalized = text
            .replace(' ', 'T')
            .let { s ->
                // PG may omit minutes in tz offset (e.g. "+02" instead of "+02:00")
                val tzIdx = s.indexOfLast { it == '+' || it == '-' }
                if (tzIdx > 10) {
                    val tz = s.substring(tzIdx)
                    if (tz.length == 3) s + ":00" else s
                } else s
            }
        return Instant.parse(normalized)
    }
}
