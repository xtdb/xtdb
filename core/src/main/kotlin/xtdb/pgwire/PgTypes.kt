@file:JvmName("PgTypes")

package xtdb.pgwire

import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.VectorReader
import xtdb.time.Interval
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.Arrays

@JvmField
val PG_EPOCH: LocalDate = LocalDate.of(2000, 1, 1)

@JvmField
val UNIX_PG_EPOCH_DIFF_DAYS: Int = PG_EPOCH.toEpochDay().toInt()

@JvmField
val UNIX_PG_EPOCH_DIFF_MICROS: Long = PG_EPOCH.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000

const val TRANSIT_OID: Long = 16384

// String utilities

fun readUtf8(ba: ByteArray): String = String(ba, StandardCharsets.UTF_8)

fun utf8(s: Any): ByteArray = s.toString().toByteArray(StandardCharsets.UTF_8)

fun escapePgArrayElement(s: String): String =
    "\"${s.replace("\\", "\\\\").replace("\"", "\\\"")}\""

// Date/time formatters

@JvmField
val ISO_LOCAL_DATE_TIME_FORMATTER_WITH_SPACE: DateTimeFormatter =
    DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .optionalStart()
        .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
        .parseStrict()
        .toFormatter()

@JvmField
val ISO_OFFSET_DATE_TIME_FORMATTER_WITH_SPACE: DateTimeFormatter =
    DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE_TIME_FORMATTER_WITH_SPACE)
        .parseLenient()
        .optionalStart()
        .appendOffset("+HH:MM:ss", "+00:00")
        .optionalEnd()
        .parseStrict()
        .toFormatter()

// Buffer/array utilities

fun calculateBufferSize(length: Int, typlen: Int): Int =
  // PG array binary format: <<dimensions, data-offset, oid, elem-count, l-bound, data>>
  // https://github.com/postgres/postgres/blob/master/src/include/utils/array.h
    4 + (4 * 4) + (typlen * length) + (4 * length)

fun binaryListHeader(length: Int, elementOid: Int, typlen: Int) =
    ByteBuffer.allocate(calculateBufferSize(length, typlen)).apply {
        putInt(1)           // dimensions
        putInt(0)           // data offset
        putInt(elementOid)
        putInt(length)
        putInt(1)           // l-bound - MUST BE 1
  }

fun binaryVariableSizeArrayHeader(elementCount: Int, oid: Int, lengthOfElements: Int): ByteBuffer =
    ByteBuffer.allocate(20 + (4 * elementCount) + lengthOfElements).apply {
        putInt(1)           // dimensions
        putInt(0)           // data offset
        putInt(oid)
        putInt(elementCount)
        putInt(1)           // l-bounds - MUST BE 1
    }

fun readBinaryIntArray(ba: ByteArray): List<Long> {
    val bb = ByteBuffer.wrap(ba)
    val dataOffset = bb.getInt(4)
    val elemOid = bb.getInt(8)
    val length = bb.getInt(12)
    var start = 24 + dataOffset  // 24 - header = 20 bytes + length of element = 4 bytes

    val (typlen, getElem) = when (elemOid) {
        23 -> 4 to { buf: ByteBuffer, pos: Int -> buf.getInt(pos).toLong() }
        20 -> 8 to { buf: ByteBuffer, pos: Int -> buf.getLong(pos) }
        else -> throw IllegalArgumentException("Unsupported element OID: $elemOid")
    }

    return buildList {
        repeat(length) {
            add(getElem(bb, start))
            start += 4 + typlen
        }
    }
}

fun readBinaryTextArray(ba: ByteArray): List<String> {
    val bb = ByteBuffer.wrap(ba)
    val dataOffset = bb.getInt(4)
    val length = bb.getInt(12)
    var start = 20 + dataOffset

    return buildList {
        repeat(length) {
            val strLen = bb.getInt(start)
            val string = readUtf8(Arrays.copyOfRange(ba, start + 4, start + 4 + strLen))
            add(string)
            start += 4 + strLen
        }
    }
}

// Vector utilities

fun VectorReader.getUnixMicros(idx: Int): Long {
    val arrowType = field.type
    val micros = getLong(idx)

    return if (arrowType is ArrowType.Timestamp && arrowType.unit == TimeUnit.NANOSECOND) {
        micros / 1000
    } else {
        micros
    }
}

fun VectorReader.intervalToIsoMicroBytes(idx: Int): ByteArray {
    val itvl = getObject(idx)
    val truncated = when (itvl) {
        is Interval -> Interval(itvl.months, itvl.days, (itvl.nanos / 1000) * 1000)
        else -> throw IllegalArgumentException("Unsupported interval type: $itvl")
    }
    return utf8(truncated)
}
