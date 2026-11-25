package xtdb.hash

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.hash.MurmurHasher.combineHashCode
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.IndirectVector
import xtdb.arrow.IntVector
import xtdb.arrow.LongVector
import xtdb.arrow.REE_VALUES_NAME
import xtdb.arrow.Relation
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.Vector
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.DATE_DAY
import xtdb.arrow.VectorType.Companion.DURATION_MICRO
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.IID
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.INTERVAL_MDM
import xtdb.arrow.VectorType.Companion.INTERVAL_MDN
import xtdb.arrow.VectorType.Companion.INTERVAL_YEAR
import xtdb.arrow.VectorType.Companion.KEYWORD
import xtdb.arrow.VectorType.Companion.OID
import xtdb.arrow.VectorType.Companion.REG_CLASS
import xtdb.arrow.VectorType.Companion.REG_PROC
import xtdb.arrow.VectorType.Companion.TIMESTAMP_MICRO
import xtdb.arrow.VectorType.Companion.TIME_NANO
import xtdb.arrow.VectorType.Companion.TRANSIT
import xtdb.arrow.VectorType.Companion.TSTZ_RANGE
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.listy
import xtdb.arrow.VectorType.Companion.mapTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.setTypeOf
import xtdb.arrow.VectorType.Null
import xtdb.arrow.VectorType.Scalar
import xtdb.expression.map.IndexHasher.Companion.hasher
import xtdb.test.AllocatorResolver
import xtdb.time.Interval
import xtdb.types.Oid
import xtdb.types.RegClass
import xtdb.types.RegProc
import xtdb.types.ZonedDateTimeRange
import xtdb.util.Hasher
import java.math.BigDecimal
import java.net.URI
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.util.UUID
import kotlin.reflect.KClass

@ExtendWith(AllocatorResolver::class)
class VectorHashesTest {

    private data class HashCase(val label: String, val open: (BufferAllocator) -> Vector) {
        constructor(label: String, type: VectorType, values: List<Any?>) :
                this(label, { al -> Vector.fromList(al, "v", type, values) })
    }

    private val zdt = Instant.parse("2024-03-05T10:15:30.123456Z").atZone(ZoneId.of("UTC"))

    private val cases = listOf(
        HashCase("i64", Null, listOf(1L, null, -5L, Long.MAX_VALUE, 0L)),
        HashCase("i32", Null, listOf(1, 2, null, Int.MIN_VALUE)),
        HashCase("i16", Null, listOf(1.toShort(), null, Short.MIN_VALUE)),
        HashCase("i8", Null, listOf(1.toByte(), null, (-3).toByte())),
        HashCase("f32", Null, listOf(1.5f, null, -0.0f, Float.NaN)),
        HashCase("f64", Null, listOf(1.0, null, -0.0, 0.0, Double.NaN, 2.5)),
        HashCase("utf8", Null, listOf("", "a", null, "a considerably longer string over sixteen bytes", "héllo")),
        HashCase("varbinary", Null, listOf(byteArrayOf(), byteArrayOf(1, 2, 3), null, ByteArray(40) { it.toByte() })),
        HashCase("fixed-size-binary", maybe(IID), listOf(ByteBuffer.wrap(ByteArray(16) { it.toByte() }), null, ByteBuffer.wrap(ByteArray(16)))),
        HashCase("bool", Null, listOf(true, null, false)),
        HashCase("date-day", maybe(DATE_DAY), listOf(LocalDate.of(2020, 1, 1), null, LocalDate.of(1969, 12, 31))),
        HashCase("date-milli", maybe(Scalar(ArrowType.Date(DateUnit.MILLISECOND))), listOf(LocalDate.of(2020, 1, 1), null)),
        HashCase("timestamp-local", maybe(TIMESTAMP_MICRO), listOf(LocalDateTime.of(2020, 1, 1, 12, 30), null)),
        HashCase("timestamp-tz", maybe(INSTANT), listOf(zdt, null, zdt.plusDays(3))),
        HashCase("time32", maybe(Scalar(ArrowType.Time(TimeUnit.MILLISECOND, 32))), listOf(LocalTime.of(10, 15), null)),
        HashCase("time64", maybe(TIME_NANO), listOf(LocalTime.of(10, 15, 30, 123), null)),
        HashCase("duration", maybe(DURATION_MICRO), listOf(Duration.ofSeconds(5), null, Duration.ofNanos(123_000))),
        HashCase("interval-year-month", maybe(INTERVAL_YEAR), listOf(Interval(14, 0, 0), null)),
        HashCase("interval-day-time", maybe(Scalar(ArrowType.Interval(IntervalUnit.DAY_TIME))), listOf(Interval(0, 3, 5_000_000), null)),
        HashCase("interval-mdn", maybe(INTERVAL_MDN), listOf(Interval(1, 2, 3), null)),
        HashCase("decimal", Null, listOf(BigDecimal("1.500"), null, BigDecimal("-22.125"))),
        HashCase("uuid", Null, listOf(UUID.randomUUID(), null, UUID.randomUUID())),
        HashCase("uri", Null, listOf(URI("http://example.com"), null, URI("foo:bar"))),
        HashCase("keyword", maybe(KEYWORD), listOf(clojure.lang.Keyword.intern("a"), null)),
        HashCase("list", Null, listOf(listOf(1L, 2L), null, listOf("x"))),
        HashCase("struct", Null, listOf(mapOf("a" to 1L), null, mapOf("a" to 2L, "b" to "x"))),
        HashCase("union", Null, listOf(1L, "a", null, 2.5, listOf(1L), true)),
        HashCase("interval-mdm", maybe(INTERVAL_MDM), listOf(Interval(1, 2, 3_000), null)),
        HashCase("map", maybe(mapTypeOf(UTF8, I64)), listOf(mapOf("a" to 1L), null, mapOf("b" to 3L, "c" to 2L))),
        HashCase("fixed-size-list", maybe(listy(ArrowType.FixedSizeList(2), I64)), listOf(listOf(1L, 2L), null, listOf(3L, null))),
        HashCase("set", maybe(setTypeOf(I64)), listOf(setOf(1L, 2L), null, emptySet<Long>())),
        HashCase("oid", maybe(OID), listOf(Oid(23), null)),
        HashCase("regclass", maybe(REG_CLASS), listOf(RegClass(1259), null)),
        HashCase("regproc", maybe(REG_PROC), listOf(RegProc(1242), null)),
        HashCase("tstz-range", maybe(TSTZ_RANGE), listOf(ZonedDateTimeRange(zdt, zdt.plusDays(1)), null, ZonedDateTimeRange(zdt, null))),
        HashCase("transit", maybe(TRANSIT), listOf(clojure.lang.Keyword.intern("a"), null, listOf(1L, "x"))),
        HashCase("run-end-encoded") { al ->
            RunEndEncodedVector(al, "v", LongVector(al, REE_VALUES_NAME, true))
                .also { vec -> listOf(10L to 3, null to 2, 20L to 1).forEach { (value, len) -> vec.writeRun(value, len) } }
        },
        HashCase("all-null", Null, listOf(null, null)),
        HashCase("empty", Null, emptyList()),
    )

    private val hashers = listOf(Hasher.Xx(), Hasher.Murmur3())

    private fun VectorReader.bulkHashes(al: BufferAllocator, hasher: Hasher, sel: IntArray?) =
        IntVector.open(al, "out", false).use { out ->
            hashTo(out, hasher, sel)
            List(out.valueCount) { out.getInt(it) }
        }

    private fun assertMatchesPerRow(vec: VectorReader, al: BufferAllocator, label: String) {
        val rowCount = vec.valueCount

        for (hasher in hashers) {
            val hasherName = hasher::class.simpleName

            assertEquals(List(rowCount) { vec.hashCode(it, hasher) }, vec.bulkHashes(al, hasher, null), "$label $hasherName")

            if (rowCount > 0)
                for (sel in listOf(intArrayOf(), intArrayOf(rowCount - 1), IntArray(2 * rowCount) { (it * 7) % rowCount }))
                    assertEquals(
                        sel.map { vec.hashCode(it, hasher) }, vec.bulkHashes(al, hasher, sel),
                        "$label $hasherName sel=${sel.toList()}"
                    )
        }
    }

    private fun KClass<out Vector>.concreteSubclasses(): List<KClass<out Vector>> =
        when {
            isSealed -> sealedSubclasses.flatMap { it.concreteSubclasses() }
            isAbstract -> error("$simpleName isn't sealed, so its subclasses can't be listed")
            else -> listOf(this)
        }

    @Test
    fun `bulk hashes append to what out already holds, and a later write appends after them`(al: BufferAllocator) {
        val xx = Hasher.Xx()

        LongVector(al, "v", false).use { vec ->
            repeat(50) { vec.writeLong(it.toLong()) }
            val perRow = List(vec.valueCount) { vec.hashCode(it, xx) }

            IntVector.open(al, "out", false).use { out ->
                out.writeInt(42)
                vec.hashTo(out, xx)
                vec.hashTo(out, xx, intArrayOf(3, 1))
                out.writeInt(7)

                assertEquals(listOf(42) + perRow + listOf(perRow[3], perRow[1]) + listOf(7), List(out.valueCount) { out.getInt(it) })
            }
        }
    }

    @Test
    fun `every concrete vector type has a hash case`(al: BufferAllocator) {
        val covered = cases.mapTo(mutableSetOf()) { case -> case.open(al).use { it::class } }

        assertEquals(emptySet<KClass<out Vector>>(), Vector::class.concreteSubclasses().toSet() - covered)
    }

    @Test
    fun `a vector's bulk hashes equal its per-row hashes, through selections and indirection`(al: BufferAllocator) {
        for ((label, open) in cases) {
            open(al).use { vec ->
                assertMatchesPerRow(vec, al, label)

                val n = vec.valueCount
                if (n > 0) {
                    assertMatchesPerRow(IndirectVector(vec, selection(IntArray(n + 1) { (n - it).coerceAtMost(n - 1) })), al, "$label selection")
                    assertMatchesPerRow(vec.withName("renamed"), al, "$label renamed")
                }
            }
        }
    }

    @Test
    fun `IndexHasher's bulk key hashes fold each key column's per-row hashes`(al: BufferAllocator) {
        val rows = List(7) { mapOf("a" to (it % 3).toLong(), "b" to if (it % 2 == 0) "s$it" else null, "c" to it * 0.5) }
        val xx = Hasher.Xx()

        Relation.openFromRows(al, rows).use { rel ->
            IntVector.open(al, "scratch", false).use { scratch ->
                for (cols in listOf(emptyList(), listOf("a"), listOf("a", "b"), listOf("b", "c", "a"), listOf("c", "b"))) {
                    val expected = List(rel.rowCount) { idx -> cols.fold(0) { acc, col -> combineHashCode(acc, rel[col].hashCode(idx, xx)) } }

                    IntVector.open(al, "out", false).use { out ->
                        out.writeInt(42)
                        rel.hasher(cols).hashTo(out, scratch)
                        assertEquals(expected, List(out.valueCount) { out.getInt(it) }, "$cols")
                    }
                }
            }
        }
    }
}
