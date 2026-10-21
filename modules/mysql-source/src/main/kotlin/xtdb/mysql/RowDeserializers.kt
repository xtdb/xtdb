package xtdb.mysql

import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.deserialization.DeleteRowsEventDataDeserializer
import com.github.shyiko.mysql.binlog.event.deserialization.UpdateRowsEventDataDeserializer
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream
import java.io.Serializable
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime

/**
 * The binlog's binary forms of the column types the library's own decoding loses information on:
 * it decodes strings without the column's charset, reads temporals through the JVM's default zone and
 * `java.util.Date` (dropping microseconds), and can't represent a TIME beyond 24h or a negative one.
 * Strings are handed back as raw bytes, the temporals as java.time values.
 */
internal object BinlogValues {

    fun string(maxLength: Int, input: ByteArrayInputStream): Serializable {
        val length = if (maxLength < 256) input.readInteger(1) else input.readInteger(2)
        return input.read(length)
    }

    fun date(input: ByteArrayInputStream): Serializable? {
        val packed = input.readInteger(3)
        return localDate(packed shr 9, (packed shr 5) and 15, packed and 31)
    }

    private fun localDate(year: Int, month: Int, day: Int): LocalDate? =
        // a zero component, or a day the month doesn't have, is MySQL's "no such date"
        if (year == 0 || month == 0 || day == 0) null
        else try { LocalDate.of(year, month, day) } catch (_: java.time.DateTimeException) { null }

    /** DATETIME(fsp) in the 5.6+ layout: 5 big-endian bytes, then `fsp` digits of fraction. */
    fun datetimeV2(fsp: Int, input: ByteArrayInputStream): Serializable? {
        val packed = bigEndian(input.read(5))
        val yearMonth = bits(packed, 1, 17, 40)
        val micros = fraction(fsp, input)

        val date = localDate(yearMonth / 13, yearMonth % 13, bits(packed, 18, 5, 40)) ?: return null
        return LocalDateTime.of(
            date.year, date.month, date.dayOfMonth,
            bits(packed, 23, 5, 40), bits(packed, 28, 6, 40), bits(packed, 34, 6, 40),
            micros * 1000,
        )
    }

    /** TIMESTAMP(fsp): seconds since the epoch, in UTC whatever the writer's zone. */
    fun timestampV2(fsp: Int, input: ByteArrayInputStream): Serializable? {
        val seconds = bigEndian(input.read(4))
        val micros = fraction(fsp, input)

        // 0 is '0000-00-00 00:00:00', below the TIMESTAMP range
        return if (seconds == 0L && micros == 0) null
        else ZonedDateTime.of(LocalDateTime.ofEpochSecond(seconds, micros * 1000, ZoneOffset.UTC), ZoneOffset.UTC)
    }

    /** TIME(fsp): sign, hours (up to 838), minutes, seconds, then `fsp` digits of fraction. */
    fun timeV2(fsp: Int, input: ByteArrayInputStream): Serializable {
        val fractionBytes = (fsp + 1) / 2
        val width = (3 + fractionBytes) * 8

        var packed = bigEndian(input.read(3 + fractionBytes))
        val negative = bits(packed, 0, 1, width) == 0

        // a negative value is stored two's-complement over the whole payload, fraction included
        if (negative) packed = (-packed) and ((1L shl width) - 1)

        val micros = bitsLong(packed, 24, fractionBytes * 8, width) * pow10(6 - 2 * fractionBytes)

        val d = Duration.ofHours(bits(packed, 2, 10, width).toLong())
            .plusMinutes(bits(packed, 12, 6, width).toLong())
            .plusSeconds(bits(packed, 18, 6, width).toLong())
            .plusNanos(micros * 1000)

        return if (negative) d.negated() else d
    }

    fun year(input: ByteArrayInputStream): Serializable? = input.readInteger(1).let { if (it == 0) null else 1900 + it }

    private fun fraction(fsp: Int, input: ByteArrayInputStream): Int {
        val bytes = (fsp + 1) / 2
        return if (bytes == 0) 0 else (bigEndian(input.read(bytes)) * pow10(6 - 2 * bytes)).toInt()
    }

    private fun pow10(n: Int): Long = if (n <= 0) 1 else 10 * pow10(n - 1)

    private fun bigEndian(bytes: ByteArray): Long = bytes.fold(0L) { acc, b -> (acc shl 8) or (b.toLong() and 0xFF) }

    private fun bitsLong(value: Long, offset: Int, count: Int, width: Int): Long =
        if (count == 0) 0 else (value shr (width - (offset + count))) and ((1L shl count) - 1)

    private fun bits(value: Long, offset: Int, count: Int, width: Int): Int = bitsLong(value, offset, count, width).toInt()
}

internal class MySqlWriteRows(tableMaps: Map<Long, TableMapEventData>) : WriteRowsEventDataDeserializer(tableMaps) {
    override fun deserializeString(length: Int, i: ByteArrayInputStream) = BinlogValues.string(length, i)
    override fun deserializeVarString(meta: Int, i: ByteArrayInputStream) = BinlogValues.string(meta, i)
    override fun deserializeDate(i: ByteArrayInputStream) = BinlogValues.date(i)
    override fun deserializeDatetimeV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.datetimeV2(meta, i)
    override fun deserializeTimestampV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.timestampV2(meta, i)
    override fun deserializeTimeV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.timeV2(meta, i)
    override fun deserializeYear(i: ByteArrayInputStream) = BinlogValues.year(i)
}

internal class MySqlUpdateRows(tableMaps: Map<Long, TableMapEventData>) : UpdateRowsEventDataDeserializer(tableMaps) {
    override fun deserializeString(length: Int, i: ByteArrayInputStream) = BinlogValues.string(length, i)
    override fun deserializeVarString(meta: Int, i: ByteArrayInputStream) = BinlogValues.string(meta, i)
    override fun deserializeDate(i: ByteArrayInputStream) = BinlogValues.date(i)
    override fun deserializeDatetimeV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.datetimeV2(meta, i)
    override fun deserializeTimestampV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.timestampV2(meta, i)
    override fun deserializeTimeV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.timeV2(meta, i)
    override fun deserializeYear(i: ByteArrayInputStream) = BinlogValues.year(i)
}

internal class MySqlDeleteRows(tableMaps: Map<Long, TableMapEventData>) : DeleteRowsEventDataDeserializer(tableMaps) {
    override fun deserializeString(length: Int, i: ByteArrayInputStream) = BinlogValues.string(length, i)
    override fun deserializeVarString(meta: Int, i: ByteArrayInputStream) = BinlogValues.string(meta, i)
    override fun deserializeDate(i: ByteArrayInputStream) = BinlogValues.date(i)
    override fun deserializeDatetimeV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.datetimeV2(meta, i)
    override fun deserializeTimestampV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.timestampV2(meta, i)
    override fun deserializeTimeV2(meta: Int, i: ByteArrayInputStream) = BinlogValues.timeV2(meta, i)
    override fun deserializeYear(i: ByteArrayInputStream) = BinlogValues.year(i)
}
