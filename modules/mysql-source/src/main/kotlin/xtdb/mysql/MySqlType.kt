package xtdb.mysql

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary
import kotlinx.serialization.json.*
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.Charset
import java.sql.ResultSet
import java.time.*
import java.util.BitSet

/**
 * What a MySQL column lands as in XT, read either from the snapshot's JDBC [ResultSet] or from what the row
 * deserializers in `RowDeserializers.kt` hand over off the binlog — both MUST return the same value for the same
 * stored value.
 */
sealed interface MySqlType {

    fun fromSnapshot(rs: ResultSet, idx: Int): Any?
    fun fromBinlog(raw: Serializable): Any?

    /** TINYINT..BIGINT. Everything fits a Long except BIGINT UNSIGNED, which is a scale-0 decimal. */
    data class Integral(val bytes: Int, val unsigned: Boolean) : MySqlType {
        private val asDecimal get() = unsigned && bytes == 8

        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? =
            if (asDecimal) rs.getBigDecimal(idx) else rs.getLong(idx).takeUnless { rs.wasNull() }

        override fun fromBinlog(raw: Serializable): Any = when (raw) {
            // every width up to INT arrives sign-extended
            is Int ->
                if (unsigned) raw.toLong() and ((1L shl (bytes * 8)) - 1) else raw.toLong()

            is Long ->
                if (asDecimal && raw < 0) BigDecimal(BigInteger.valueOf(raw).add(BigInteger.ONE.shiftLeft(64)))
                else if (asDecimal) BigDecimal.valueOf(raw)
                else raw

            else -> unexpected(raw)
        }
    }

    data object Single : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getFloat(idx).takeUnless { rs.wasNull() }
        override fun fromBinlog(raw: Serializable): Any = raw
    }

    data object Real : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getDouble(idx).takeUnless { rs.wasNull() }
        override fun fromBinlog(raw: Serializable): Any = raw
    }

    data object Decimal : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getBigDecimal(idx)
        override fun fromBinlog(raw: Serializable): Any = raw
    }

    data class Text(val charset: Charset) : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getString(idx)
        override fun fromBinlog(raw: Serializable): Any = String(raw as ByteArray, charset)
    }

    /**
     * BINARY/VARBINARY/BLOB — and by the same token, GEOMETRY, which is passed through in MySQL's internal form.
     *
     * The binlog drops the trailing zeros BINARY(n) pads with, so a [fixedLength] column is padded back out to n.
     */
    data class Binary(val fixedLength: Int? = null) : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getBytes(idx)
        override fun fromBinlog(raw: Serializable): Any =
            (raw as ByteArray).let { if (fixedLength != null && it.size < fixedLength) it.copyOf(fixedLength) else it }
    }

    data class Bit(val bits: Int) : MySqlType {
        private val byteLength get() = (bits + 7) / 8

        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? =
            rs.getBytes(idx)?.let { bytes -> if (bits == 1) bytes[0].toInt() != 0 else bytes }

        override fun fromBinlog(raw: Serializable): Any {
            val set = raw as BitSet
            if (bits == 1) return set.get(0)

            val bytes = ByteArray(byteLength)
            set.stream().forEach { i -> bytes[byteLength - 1 - i / 8] = (bytes[byteLength - 1 - i / 8].toInt() or (1 shl (i % 8))).toByte() }
            return bytes
        }
    }

    data class Enumeration(val values: List<String>) : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getString(idx)

        // index 0 is the error value '', which a non-strict insert of an unknown member stores
        override fun fromBinlog(raw: Serializable): Any = (raw as Int).let { if (it == 0) "" else values[it - 1] }
    }

    data class Members(val values: List<String>) : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? =
            rs.getString(idx)?.let { s -> if (s.isEmpty()) emptySet() else s.split(',').toSet() }

        override fun fromBinlog(raw: Serializable): Any {
            val mask = raw as Long
            return values.indices.filter { mask and (1L shl it) != 0L }.mapTo(LinkedHashSet()) { values[it] }
        }
    }

    /** `0000-00-00` and its partly-zero cousins have no calendar date to land as, so they are null. */
    data object Date : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getString(idx)?.let { TemporalText.date(it) }
        override fun fromBinlog(raw: Serializable): Any = raw
    }

    /** Zone-less, so a LocalDateTime — the wall-clock reading, however the writer's session was configured. */
    data object Datetime : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getString(idx)?.let { TemporalText.datetime(it) }
        override fun fromBinlog(raw: Serializable): Any = raw
    }

    /** An instant, UTC-normalised on the way out of the server: the snapshot session pins `time_zone` to UTC. */
    data object Timestamp : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? =
            rs.getString(idx)?.let { TemporalText.datetime(it)?.atZone(ZoneOffset.UTC) }

        override fun fromBinlog(raw: Serializable): Any = raw
    }

    /** MySQL's TIME is an elapsed duration (±838:59:59), not a time of day. */
    data object Time : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getString(idx)?.let { TemporalText.time(it) }
        override fun fromBinlog(raw: Serializable): Any = raw
    }

    data object Year : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? =
            rs.getString(idx)?.toLong()?.takeUnless { it == 0L }

        override fun fromBinlog(raw: Serializable): Any = (raw as Int).toLong()
    }

    data object JsonDoc : MySqlType {
        override fun fromSnapshot(rs: ResultSet, idx: Int): Any? = rs.getString(idx)?.let(::parse)
        override fun fromBinlog(raw: Serializable): Any? = parse(JsonBinary.parseAsString(raw as ByteArray))

        private fun parse(text: String): Any? = Json.parseToJsonElement(text).toValue()

        private fun JsonElement.toValue(): Any? = when (this) {
            is JsonNull -> null
            is JsonObject -> entries.associateTo(LinkedHashMap()) { (k, v) -> k to v.toValue() }
            is JsonArray -> map { it.toValue() }
            is JsonPrimitive ->
                if (isString) content
                else content.toBooleanStrictOrNull() ?: content.toLongOrNull() ?: content.toDouble()
        }
    }

    companion object {
        private fun unexpected(raw: Any): Nothing = error("unexpected binlog value ${raw.javaClass.name}")
    }
}

/** Parses the text the server renders temporal values as; the binlog side reads the binary form instead. */
internal object TemporalText {
    fun date(s: String): LocalDate? =
        try { LocalDate.parse(s) } catch (_: DateTimeException) { null }

    fun datetime(s: String): LocalDateTime? =
        try { LocalDateTime.parse(s.replace(' ', 'T')) } catch (_: DateTimeException) { null }

    /** `[-]HHH:MM:SS[.ffffff]` */
    fun time(s: String): Duration {
        val negative = s.startsWith('-')
        val (hms, frac) = s.removePrefix("-").split('.', limit = 2).let { it[0] to it.getOrElse(1) { "" } }
        val (h, m, sec) = hms.split(':').map(String::toLong)
        val micros = frac.padEnd(6, '0').take(6).toLong()

        val d = Duration.ofHours(h).plusMinutes(m).plusSeconds(sec).plusNanos(micros * 1000)
        return if (negative) d.negated() else d
    }
}
