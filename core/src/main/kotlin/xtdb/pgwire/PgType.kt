package xtdb.pgwire

import org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE
import org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor
import org.apache.commons.codec.binary.Hex
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.BOOL
import xtdb.arrow.VectorType.Companion.F32
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I16
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.KEYWORD
import xtdb.arrow.VectorType.Companion.OID
import xtdb.arrow.VectorType.Companion.REG_CLASS
import xtdb.arrow.VectorType.Companion.REG_PROC
import xtdb.arrow.VectorType.Companion.TSTZ_RANGE
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.UUID
import xtdb.arrow.VectorType.Companion.VAR_BINARY
import xtdb.arrow.unsupported
import xtdb.decode as jsonDecode
import xtdb.encode as jsonEncode
import xtdb.encodeJsonLd
import xtdb.pg.codec.decode
import xtdb.pg.codec.encode
import xtdb.pgwire.TypCategory.*
import xtdb.time.InstantUtil
import xtdb.time.Interval
import xtdb.time.asInterval
import xtdb.time.asSqlTimestamp
import xtdb.time.microsAsInstant
import xtdb.types.ZonedDateTimeRange
import xtdb.util.TransitFormat
import xtdb.util.readTransit
import xtdb.util.writeTransit
import xtdb.vector.extensions.*
import java.io.ByteArrayInputStream
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.*
import java.time.format.DateTimeFormatter

internal typealias Oid = Int

enum class TypCategory(val chr: Char) {
    NUMERIC('N'),
    STRING('S'),
    BOOLEAN('B'),
    DATE('D'),
    TIMESPAN('T'),
    USER_DEFINED('U'),
    ARRAY('A'),
    RANGE('R'),
}

internal typealias PgSessionEnv = Map<String, String>

sealed class PgType(
    val typname: String,
    val xtType: VectorType?,
    val oid: Oid,
    val typlen: Int = -1,
    val typcategory: TypCategory,
    val typsend: String,
    val typreceive: String,
    val typarray: Oid? = null,
    val typelem: Oid? = null,
    val typinput: String? = null,
    val typoutput: String? = null,
) {
    open fun readBinary(data: ByteArray): Any? = unsupported("readBinary")
    open fun readText(data: ByteArray): Any? = unsupported("readText")
    open fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray? = unsupported("writeBinary")
    open fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray? = unsupported("writeText")

    // Default type for unknown parameters
    data object Default : PgType(
        typname = "default",
        xtType = UTF8,
        oid = 0,
        typcategory = STRING,
        typsend = "",
        typreceive = "",
    ) {
        override fun readBinary(data: ByteArray): String = readUtf8(data)

        override fun readText(data: ByteArray): String = readUtf8(data)
    }

    // Null type - delegates to Text for OID/serialization, but distinct for comparison purposes
    data object Null : PgType(
        typname = "null",
        xtType = VectorType.Null,
        oid = 25, // same as Text
        typcategory = STRING,
        typsend = "textsend",
        typreceive = "textrecv",
    ) {
        override fun readBinary(data: ByteArray): String = Text.readBinary(data)
        override fun readText(data: ByteArray): String = Text.readText(data)
        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int) = Text.writeBinary(env, rdr, idx)
        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int) = Text.writeText(env, rdr, idx)
    }

    data object Int8 : PgType(
        typname = "int8",
        xtType = I64,
        oid = 20,
        typlen = 8,
        typcategory = NUMERIC,
        typsend = "int8send",
        typreceive = "int8recv",
        typarray = 1016,
    ) {
        override fun readBinary(data: ByteArray): Long = ByteBuffer.wrap(data).long

        override fun readText(data: ByteArray): Long = readUtf8(data).toLong()

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            ByteBuffer.allocate(typlen).putLong(rdr.getLong(idx)).array()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = utf8(rdr.getLong(idx))
    }

    data object Int4 : PgType(
        typname = "int4",
        xtType = I32,
        oid = 23,
        typlen = 4,
        typcategory = NUMERIC,
        typsend = "int4send",
        typreceive = "int4recv",
        typarray = 1007,
    ) {
        override fun readBinary(data: ByteArray): Int = ByteBuffer.wrap(data).int

        override fun readText(data: ByteArray): Int = readUtf8(data).toInt()

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            ByteBuffer.allocate(typlen).putInt(rdr.getInt(idx)).array()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = utf8(rdr.getInt(idx))
    }

    data object Int2 : PgType(
        typname = "int2",
        xtType = I16,
        oid = 21,
        typlen = 2,
        typcategory = NUMERIC,
        typsend = "int2send",
        typreceive = "int2recv",
    ) {
        override fun readBinary(data: ByteArray): Short = ByteBuffer.wrap(data).short

        override fun readText(data: ByteArray): Short = readUtf8(data).toShort()

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            ByteBuffer.allocate(typlen).putShort(rdr.getShort(idx)).array()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = utf8(rdr.getShort(idx))
    }

    data object Float4 : PgType(
        typname = "float4",
        xtType = F32,
        oid = 700,
        typlen = 4,
        typcategory = NUMERIC,
        typsend = "float4send",
        typreceive = "float4recv",
    ) {
        override fun readBinary(data: ByteArray): Float = ByteBuffer.wrap(data).float

        override fun readText(data: ByteArray): Float = readUtf8(data).toFloat()

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            ByteBuffer.allocate(typlen).putFloat(rdr.getFloat(idx)).array()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = utf8(rdr.getFloat(idx))
    }

    data object Float8 : PgType(
        typname = "float8",
        xtType = F64,
        oid = 701,
        typlen = 8,
        typcategory = NUMERIC,
        typsend = "float8send",
        typreceive = "float8recv",
    ) {
        override fun readBinary(data: ByteArray): Double = ByteBuffer.wrap(data).double

        override fun readText(data: ByteArray): Double = readUtf8(data).toDouble()

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            ByteBuffer.allocate(typlen).putDouble(rdr.getDouble(idx)).array()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = utf8(rdr.getDouble(idx))
    }

    data object Bool : PgType(
        typname = "boolean",
        xtType = BOOL,
        oid = 16,
        typlen = 1,
        typcategory = BOOLEAN,
        typsend = "boolsend",
        typreceive = "boolrecv",
    ) {
        override fun readBinary(data: ByteArray): Boolean = when (ByteBuffer.wrap(data).get().toInt()) {
            1 -> true
            0 -> false
            else -> throw IllegalArgumentException("Invalid binary boolean value")
        }

        override fun readText(data: ByteArray): Boolean =
            when (readUtf8(data).trim().lowercase()) {
                "tr", "yes", "tru", "true", "on", "ye", "t", "y", "1" -> true
                "f", "fa", "fal", "fals", "false", "no", "n", "off", "0" -> false
                else -> throw IllegalArgumentException("Invalid boolean value")
            }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            byteArrayOf(if (rdr.getBoolean(idx)) 1 else 0)

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(if (rdr.getBoolean(idx)) "t" else "f")
    }

    data object Text : PgType(
        typname = "text",
        xtType = UTF8,
        oid = 25,
        typcategory = STRING,
        typsend = "textsend",
        typreceive = "textrecv",
        typarray = 1009,
    ) {
        override fun readBinary(data: ByteArray): String = readUtf8(data)

        override fun readText(data: ByteArray): String = readUtf8(data)

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            rdr.getBytes(idx).let { bb -> ByteArray(bb.remaining()).also { bb.get(it) } }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = writeBinary(env, rdr, idx)
    }

    data object VarChar : PgType(
        typname = "varchar",
        xtType = UTF8,
        oid = 1043,
        typcategory = STRING,
        typsend = "varcharsend",
        typreceive = "varcharrecv",
    ) {
        override fun readBinary(data: ByteArray): String = readUtf8(data)

        override fun readText(data: ByteArray): String = readUtf8(data)

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            rdr.getBytes(idx).let { bb -> ByteArray(bb.remaining()).also { bb.get(it) } }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = writeBinary(env, rdr, idx)
    }

    data object Numeric : PgType(
        typname = "numeric",
        xtType = null, // decimal has precision/scale
        oid = 1700,
        typcategory = NUMERIC,
        typsend = "numeric_send",
        typreceive = "numeric_recv",
    ) {
        override fun readBinary(data: ByteArray): BigDecimal = decode(ByteBuffer.wrap(data))

        override fun readText(data: ByteArray): BigDecimal = BigDecimal(readUtf8(data))

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            encode(rdr.getObject(idx) as BigDecimal).array()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(rdr.getObject(idx)!!)
    }

    data object Timestamp : PgType(
        typname = "timestamp",
        xtType = VectorType.TIMESTAMP_MICRO,
        oid = 1114,
        typlen = 8,
        typcategory = DATE,
        typsend = "timestamp_send",
        typreceive = "timestamp_recv",
    ) {
        override fun readBinary(data: ByteArray): LocalDateTime {
            val micros = ByteBuffer.wrap(data).long + UNIX_PG_EPOCH_DIFF_MICROS
            return LocalDateTime.ofInstant(micros.microsAsInstant, ZoneId.of("UTC"))
        }

        override fun readText(data: ByteArray): LocalDateTime {
            val text = readUtf8(data)
            val res = text.asSqlTimestamp()
            return res as? LocalDateTime
                ?: throw IllegalArgumentException("Cannot parse '$text' as timestamp")
        }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val micros = rdr.getUnixMicros(idx) - UNIX_PG_EPOCH_DIFF_MICROS
            return ByteBuffer.allocate(8).putLong(micros).array()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val ldt = rdr.getObject(idx) as LocalDateTime
            return utf8(
                if (env["datestyle"] == "iso8601") ldt.toString()
                else ldt.format(ISO_OFFSET_DATE_TIME_FORMATTER_WITH_SPACE)
            )
        }
    }

    data object TimestampTz : PgType(
        typname = "timestamptz",
        xtType = VectorType.INSTANT,
        oid = 1184,
        typlen = 8,
        typcategory = DATE,
        typsend = "timestamptz_send",
        typreceive = "timestamptz_recv",
    ) {
        override fun readBinary(data: ByteArray): OffsetDateTime {
            val micros = ByteBuffer.wrap(data).long + UNIX_PG_EPOCH_DIFF_MICROS
            return OffsetDateTime.ofInstant(micros.microsAsInstant, ZoneId.of("UTC"))
        }

        override fun readText(data: ByteArray): Any {
            val text = readUtf8(data)
            val res = text.asSqlTimestamp()
            return when (res) {
                is ZonedDateTime, is OffsetDateTime -> res
                else -> throw IllegalArgumentException("Cannot parse '$text' as timestamptz")
            }
        }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray? {
            val zdt = rdr.getObject(idx) as? ZonedDateTime ?: return null
            val unixMicros = with(InstantUtil) { zdt.toInstant().asMicros }
            val micros = unixMicros - UNIX_PG_EPOCH_DIFF_MICROS
            return ByteBuffer.allocate(8).putLong(micros).array()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray? {
            val zdt = rdr.getObject(idx) as? ZonedDateTime ?: return null
            return utf8(
                if (env["datestyle"] == "iso8601") zdt.toString()
                else zdt.format(ISO_OFFSET_DATE_TIME_FORMATTER_WITH_SPACE)
            )
        }
    }

    data object Date : PgType(
        typname = "date",
        xtType = VectorType.DATE_DAY,
        oid = 1082,
        typlen = 4,
        typcategory = DATE,
        typsend = "date_send",
        typreceive = "date_recv",
    ) {
        override fun readBinary(data: ByteArray): LocalDate {
            val days = ByteBuffer.wrap(data).int + UNIX_PG_EPOCH_DIFF_DAYS
            return LocalDate.ofEpochDay(days.toLong())
        }

        override fun readText(data: ByteArray): LocalDate = LocalDate.parse(readUtf8(data))

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val days = rdr.getInt(idx) - UNIX_PG_EPOCH_DIFF_DAYS
            return ByteBuffer.allocate(4).putInt(days).array()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val date = rdr.getObject(idx) as LocalDate
            return utf8(date.format(DateTimeFormatter.ISO_LOCAL_DATE))
        }
    }

    data object Time : PgType(
        typname = "time",
        xtType = VectorType.TIME_MICRO,
        oid = 1083,
        typlen = 8,
        typcategory = DATE,
        typsend = "time_send",
        typreceive = "time_recv",
    ) {
        override fun readBinary(data: ByteArray): LocalTime {
            val micros = ByteBuffer.wrap(data).long
            return LocalTime.ofNanoOfDay(micros * 1000)
        }

        override fun readText(data: ByteArray): LocalTime = LocalTime.parse(readUtf8(data))

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val time = rdr.getObject(idx) as LocalTime
            val micros = time.toNanoOfDay() / 1000
            return ByteBuffer.allocate(8).putLong(micros).array()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val time = rdr.getObject(idx) as LocalTime
            return utf8(time.format(DateTimeFormatter.ISO_LOCAL_TIME))
        }
    }

    data object PgInterval : PgType(
        typname = "interval",
        xtType = VectorType.INTERVAL_MDM,
        oid = 1186,
        typlen = 16,
        typcategory = TIMESPAN,
        typsend = "interval_send",
        typreceive = "interval_recv",
    ) {
        override fun readBinary(data: ByteArray): Nothing =
            throw IllegalArgumentException("Interval parameters currently unsupported")

        override fun readText(data: ByteArray): Interval =
            readUtf8(data).asInterval()

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            rdr.intervalToIsoMicroBytes(idx)

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            rdr.intervalToIsoMicroBytes(idx)
    }

    data object PgDuration : PgType(
        typname = "duration",
        xtType = VectorType.DURATION_MICRO,
        oid = 11112,
        typlen = 8,
        typcategory = TIMESPAN,
        typsend = "duration_send",
        typreceive = "duration_recv",
    ) {
        override fun readText(data: ByteArray): Duration =
            Duration.parse(readUtf8(data))

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(rdr.getObject(idx)!!)
    }

    data object TsTzRange : PgType(
        typname = "tstz-range",
        xtType = TSTZ_RANGE,
        oid = 3910,
        typcategory = RANGE,
        typsend = "range_send",
        typreceive = "range_recv",
    ) {
        private fun parseDateTime(s: String?): ZonedDateTime? {
            if (s.isNullOrEmpty()) return null
            return when (val ts = s.asSqlTimestamp()) {
                is ZonedDateTime -> ts
                is OffsetDateTime -> ts.toZonedDateTime()
                else -> null
            }
        }

        override fun readBinary(data: ByteArray): ZonedDateTimeRange? = readText(data)

        override fun readText(data: ByteArray): ZonedDateTimeRange? {
            val s = readUtf8(data)
            val match = Regex("\\[([^,]*),([^)]*)\\)").matchEntire(s) ?: return null
            val from = parseDateTime(match.groupValues[1])
            val to = parseDateTime(match.groupValues[2])
            return ZonedDateTimeRange(from, to)
        }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val range = rdr.getObject(idx) as ZonedDateTimeRange
            return ("[${range.from?.format(ISO_OFFSET_DATE_TIME_FORMATTER_WITH_SPACE) ?: ""}" +
                    ",${range.to?.format(ISO_OFFSET_DATE_TIME_FORMATTER_WITH_SPACE) ?: ""})").toByteArray()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val range = rdr.getObject(idx) as ZonedDateTimeRange
            return utf8(
                "[${range.from?.format(ISO_OFFSET_DATE_TIME_FORMATTER_WITH_SPACE) ?: ""}" +
                        ",${range.to?.format(ISO_OFFSET_DATE_TIME_FORMATTER_WITH_SPACE) ?: ""})"
            )
        }
    }

    data object Bytes : PgType(
        typname = "bytea",
        xtType = VAR_BINARY,
        oid = 17,
        typcategory = USER_DEFINED,
        typsend = "byteasend",
        typreceive = "bytearecv",
    ) {
        override fun readText(data: ByteArray): ByteArray {
            val s = readUtf8(data)
            return if (s.startsWith("\\x")) Hex.decodeHex(s.substring(2)) else data
        }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            rdr.getBytes(idx).let { bb -> ByteArray(bb.remaining()).also { bb.get(it) } }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val bb = rdr.getBytes(idx)
            val ba = ByteArray(bb.remaining())
            bb.get(ba)
            return utf8("\\x" + Hex.encodeHexString(ba))
        }
    }

    // Array types

    data object Int4s : PgType(
        typname = "_int4",
        xtType = null,
        oid = 1007,
        typcategory = ARRAY,
        typsend = "array_send",
        typreceive = "array_recv",
        typelem = 23,
        typinput = "array_in",
        typoutput = "array_out",
    ) {
        private const val TYPLEN = 4

        override fun readBinary(data: ByteArray): List<Long> = readBinaryIntArray(data)

        override fun readText(data: ByteArray): List<Int> {
            val s = readUtf8(data).trim()
            if (s.isEmpty() || s == "{}") return emptyList()
            return s.substring(1, s.length - 1).split(",").map { it.toInt() }
        }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val list = rdr.getObject(idx) as List<*>
            val bb = binaryListHeader(list.size, typelem!!, TYPLEN)
            for (elem in list) {
                bb.putInt(TYPLEN)
                bb.putInt(elem as Int)
            }
            return bb.array()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val list = rdr.getObject(idx) as List<*>
            val sb = StringBuilder("{")
            for (elem in list) {
                sb.append(elem ?: "NULL")
                sb.append(",")
            }
            if (list.isNotEmpty()) sb.setCharAt(sb.length - 1, '}') else sb.append("}")
            return utf8(sb.toString())
        }
    }

    data object Int8s : PgType(
        typname = "_int8",
        xtType = null,
        oid = 1016,
        typcategory = ARRAY,
        typsend = "array_send",
        typreceive = "array_recv",
        typelem = 20,
        typinput = "array_in",
        typoutput = "array_out",
    ) {
        private const val TYPLEN = 8

        override fun readBinary(data: ByteArray): List<Long> = readBinaryIntArray(data)

        override fun readText(data: ByteArray): List<Long> {
            val s = readUtf8(data).trim()
            if (s.isEmpty() || s == "{}") return emptyList()
            return s.substring(1, s.length - 1).split(",").map { it.toLong() }
        }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val list = rdr.getObject(idx) as List<*>
            val bb = binaryListHeader(list.size, typelem!!, TYPLEN)
            for (elem in list) {
                bb.putInt(TYPLEN)
                bb.putLong(elem as Long)
            }
            return bb.array()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val list = rdr.getObject(idx) as List<*>
            val sb = StringBuilder("{")
            for (elem in list) {
                sb.append(elem ?: "NULL")
                sb.append(",")
            }
            if (list.isNotEmpty()) sb.setCharAt(sb.length - 1, '}') else sb.append("}")
            return utf8(sb.toString())
        }
    }

    data object Texts : PgType(
        typname = "_text",
        xtType = null,
        oid = 1009,
        typcategory = ARRAY,
        typsend = "array_send",
        typreceive = "array_recv",
        typelem = 25,
        typinput = "array_in",
        typoutput = "array_out",
    ) {
        override fun readBinary(data: ByteArray): List<String> = readBinaryTextArray(data)

        override fun readText(data: ByteArray): List<String> {
            val s = readUtf8(data).trim()
            if (s.isEmpty() || s == "{}") return emptyList()
            return s.substring(1, s.length - 1).split(",")
        }

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val list = rdr.getObject(idx) as List<*>
            val elemBytes = list.map { if (it == null) null else utf8(it) }
            val totalLen = elemBytes.sumOf { it?.size ?: 0 }
            val bb = binaryVariableSizeArrayHeader(list.size, typelem!!, totalLen)
            for (bytes in elemBytes) {
                if (bytes == null) {
                    bb.putInt(-1)
                } else {
                    bb.putInt(bytes.size)
                    bb.put(bytes)
                }
            }
            return bb.array()
        }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray {
            val list = rdr.getObject(idx) as List<*>
            val sb = StringBuilder("{")
            for (elem in list) {
                if (elem == null) {
                    sb.append("NULL")
                } else {
                    sb.append(escapePgArrayElement(elem.toString()))
                }
                sb.append(",")
            }
            if (list.isNotEmpty()) sb.setCharAt(sb.length - 1, '}') else sb.append("}")
            return utf8(sb.toString())
        }
    }

    // Extension types

    data object Uuid : PgType(
        typname = "uuid",
        xtType = UUID,
        oid = 2950,
        typlen = 16,
        typcategory = USER_DEFINED,
        typsend = "uuid_send",
        typreceive = "uuid_recv",
        typinput = "uuid_in",
        typoutput = "uuid_out",
    ) {
        private fun ByteBuffer.toUuid(): java.util.UUID =
            java.util.UUID(long, long)

        override fun readBinary(data: ByteArray): java.util.UUID =
            ByteBuffer.wrap(data).toUuid()

        override fun readText(data: ByteArray): java.util.UUID =
            java.util.UUID.fromString(readUtf8(data))

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            ByteArray(16).also { rdr.getBytes(idx).get(it) }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(rdr.getBytes(idx).toUuid())
    }

    data object Keyword : PgType(
        typname = "keyword",
        xtType = KEYWORD,
        oid = 11111,
        typcategory = STRING,
        typsend = "keywordsend",
        typreceive = "keywordrecv",
    ) {
        override fun readBinary(data: ByteArray): String = readUtf8(data)

        override fun readText(data: ByteArray): String = readUtf8(data)

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            rdr.getBytes(idx).let { bb -> ByteArray(bb.remaining()).also { bb.get(it) } }

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray = writeBinary(env, rdr, idx)
    }

    data object PgOid : PgType(
        typname = "oid",
        xtType = OID,
        oid = 26,
        typcategory = NUMERIC,
        typsend = "oidsend",
        typreceive = "oidrecv",
    ) {
        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(Integer.toUnsignedString(rdr.getInt(idx)))

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(Integer.toUnsignedString(rdr.getInt(idx)))
    }

    data object RegClass : PgType(
        typname = "regclass",
        xtType = REG_CLASS,
        oid = 2205,
        typcategory = NUMERIC,
        typsend = "regclasssend",
        typreceive = "regclassrecv",
    ) {
        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(Integer.toUnsignedString(rdr.getInt(idx)))

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(Integer.toUnsignedString(rdr.getInt(idx)))
    }

    data object RegProc : PgType(
        typname = "regproc",
        xtType = REG_PROC,
        oid = 24,
        typcategory = NUMERIC,
        typsend = "regprocsend",
        typreceive = "regprocrecv",
    ) {
        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(Integer.toUnsignedString(rdr.getInt(idx)))

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            utf8(Integer.toUnsignedString(rdr.getInt(idx)))
    }

    data object Json : PgType(
        typname = "json",
        xtType = null,
        oid = 114,
        typcategory = USER_DEFINED,
        typsend = "json_send",
        typreceive = "json_recv",
    ) {
        override fun readBinary(data: ByteArray): Any = jsonDecode(ByteArrayInputStream(data))

        override fun readText(data: ByteArray): Any = jsonDecode(ByteArrayInputStream(data))

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int) =
            jsonEncode(rdr.getObject(idx)!!).toByteArray()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int) =
            jsonEncode(rdr.getObject(idx)!!).toByteArray()
    }

    data object JsonLd : PgType(
        typname = "json",
        xtType = null,
        oid = 114,
        typcategory = USER_DEFINED,
        typsend = "json_send",
        typreceive = "json_recv",
    ) {
        override fun readBinary(data: ByteArray): Any = jsonDecode(ByteArrayInputStream(data))

        override fun readText(data: ByteArray): Any = jsonDecode(ByteArrayInputStream(data))

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int) =
            encodeJsonLd(rdr.getObject(idx)!!).toByteArray()

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int) =
            encodeJsonLd(rdr.getObject(idx)!!).toByteArray()
    }

    data object Jsonb : PgType(
        typname = "jsonb",
        xtType = null,
        oid = 3802,
        typcategory = USER_DEFINED,
        typsend = "jsonb_send",
        typreceive = "jsonb_recv",
    ) {
        override fun readBinary(data: ByteArray): Any = jsonDecode(ByteArrayInputStream(data))

        override fun readText(data: ByteArray): Any = jsonDecode(ByteArrayInputStream(data))
    }

    data object Transit : PgType(
        typname = "transit",
        xtType = null,
        oid = TRANSIT_OID.toInt(),
        typcategory = USER_DEFINED,
        typsend = "transit_send",
        typreceive = "transit_recv",
    ) {
        override fun readBinary(data: ByteArray): Any? = readTransit(data, TransitFormat.JSON)

        override fun readText(data: ByteArray): Any? = readTransit(data, TransitFormat.JSON)

        override fun writeBinary(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            writeTransit(rdr.getObject(idx)!!, TransitFormat.JSON)

        override fun writeText(env: PgSessionEnv, rdr: VectorReader, idx: Int): ByteArray =
            writeTransit(rdr.getObject(idx)!!, TransitFormat.JSON)
    }

    companion object {
        @JvmField val PG_DEFAULT = Default
        @JvmField val PG_NULL = Null
        @JvmField val PG_TEXT = Text
        @JvmField val PG_VARCHAR = VarChar
        @JvmField val PG_TIMESTAMP = Timestamp
        @JvmField val PG_TIMESTAMPTZ = TimestampTz
        @JvmField val PG_DATE = Date
        @JvmField val PG_JSON = Json
        @JvmField val PG_JSON_LD = JsonLd
        @JvmField val PG_TRANSIT = Transit
        @JvmField val PG_UUID = Uuid

        // All concrete PgTypes (excluding Default and Null which are special)
        @JvmField
        val values: List<PgType> = listOf(
            Int8, Int4, Int2, Float4, Float8, Bool, Text, VarChar, Numeric,
            Timestamp, TimestampTz, Date, Time, PgInterval, PgDuration, TsTzRange,
            Bytes, Int4s, Int8s, Texts,
            Uuid, Keyword, PgOid, RegClass, RegProc, Json, Jsonb, Transit,
        )

        private val byOid: Map<Oid, PgType> = (values + Default).associateBy { it.oid }

        @JvmStatic
        fun fromOid(oid: Oid): PgType? = byOid[oid]

        @JvmStatic
        fun fromXtType(xtType: VectorType): PgType = xtType.arrowType.accept(object : ArrowTypeVisitor<PgType> {
            override fun visit(type: ArrowType.Null) = Null
            override fun visit(type: ArrowType.Struct) = Default
            override fun visit(type: ArrowType.List) = listPgType(xtType)
            override fun visit(type: ArrowType.LargeList) = unsupported("LargeList")
            override fun visit(type: ArrowType.FixedSizeList) = listPgType(xtType)
            override fun visit(type: ArrowType.ListView) = unsupported("ListView")
            override fun visit(type: ArrowType.LargeListView) = unsupported("LargeListView")
            override fun visit(type: ArrowType.Union) = error("union should be resolved before here")
            override fun visit(type: ArrowType.Map) = Default

            override fun visit(type: ArrowType.Int) = when (type.bitWidth) {
                16 -> Int2
                32 -> Int4
                64 -> Int8
                else -> error("unexpected bit-width: ${type.bitWidth}")
            }

            override fun visit(type: ArrowType.FloatingPoint) = when (type.precision) {
                SINGLE -> Float4
                DOUBLE -> Float8
                else -> error("unexpected precision: ${type.precision}")
            }

            override fun visit(type: ArrowType.Utf8) = Text
            override fun visit(type: ArrowType.Utf8View) = Text
            override fun visit(type: ArrowType.LargeUtf8) = Text
            override fun visit(type: ArrowType.Binary) = Bytes
            override fun visit(type: ArrowType.BinaryView) = Bytes
            override fun visit(type: ArrowType.LargeBinary) = Bytes
            override fun visit(type: ArrowType.FixedSizeBinary) = Bytes
            override fun visit(type: ArrowType.Bool) = Bool
            override fun visit(type: ArrowType.Decimal) = Numeric
            override fun visit(type: ArrowType.Date) = Date
            override fun visit(type: ArrowType.Time) = Time
            override fun visit(type: ArrowType.Timestamp) = if (type.timezone == null) Timestamp else TimestampTz
            override fun visit(type: ArrowType.Interval) = PgInterval
            override fun visit(type: ArrowType.Duration) = PgDuration
            override fun visit(type: ArrowType.RunEndEncoded) = Default

            override fun visit(type: ArrowType.ExtensionType) = when (type) {
                is UuidType -> Uuid
                is KeywordType -> Keyword
                is OidType -> PgOid
                is RegClassType -> RegClass
                is RegProcType -> RegProc
                is TsTzRangeType -> TsTzRange
                is TransitType -> Transit
                is IntervalMDMType -> PgInterval
                is UriType -> Default
                is SetType -> Default
                else -> error("unknown extension type: $type")
            }
        })

        private fun listPgType(xtType: VectorType): PgType {
            val elType = xtType.asMono.firstChildOrNull?.arrowType ?: return Default

            return when (elType) {
                is ArrowType.Int -> when (elType.bitWidth) {
                    32 -> Int4s
                    64 -> Int8s
                    else -> Default
                }
                is ArrowType.Utf8 -> Texts
                else -> Default
            }
        }

        private val INTS = setOf(Int2, Int4, Int8)
        private val FLOATS = setOf(Float4, Float8)
        private val INT_ARRAYS = setOf(Int4s, Int8s)

        private fun unifyPgTypes(pgTypes: Set<PgType>): PgType = when {
            pgTypes.isEmpty() -> Null
            pgTypes.size == 1 -> pgTypes.first()
            pgTypes.all { it in INTS } -> Int8
            pgTypes.all { it in FLOATS } -> Float8
            pgTypes.all { it in INT_ARRAYS } -> Int8s
            else -> Default
        }

        @JvmStatic
        fun fromVectorType(type: VectorType): PgType {
            val pgTypes = type.legs.mapTo(mutableSetOf()) { fromXtType(it) }.minus(Null)
            return unifyPgTypes(pgTypes)
        }
    }
}
