package xtdb.arrow

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorType.Companion.INTERVAL_MDN
import xtdb.arrow.VectorType.Companion.INTERVAL_YEAR
import xtdb.arrow.VectorType.Companion.KEYWORD
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.setTypeOf
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Null
import xtdb.arrow.VectorType.Scalar
import xtdb.test.AllocatorResolver
import xtdb.time.Interval
import java.math.BigDecimal
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.function.IntBinaryOperator

@ExtendWith(AllocatorResolver::class)
class RowComparatorsTest {

    private val utc = ZoneId.of("UTC")

    private fun open(al: BufferAllocator, values: List<Any?>, type: VectorType = Null) = Vector.fromList(al, type, values)

    private fun IntBinaryOperator.table(l: VectorReader, r: VectorReader) =
        List(l.valueCount) { li -> List(r.valueCount) { ri -> applyAsInt(li, ri) } }

    private fun equi(al: BufferAllocator, l: List<Any?>, r: List<Any?>, nullsEqual: Boolean = false, zone: ZoneId = utc) =
        open(al, l).use { lv -> open(al, r).use { rv -> lv.equiComparator(rv, nullsEqual, zone).table(lv, rv) } }

    private fun order(al: BufferAllocator, l: List<Any?>, r: List<Any?>, nullsFirst: Boolean = false, zone: ZoneId = utc) =
        open(al, l).use { lv -> open(al, r).use { rv -> lv.orderComparator(rv, nullsFirst, zone).table(lv, rv).map { row -> row.map(Integer::signum) } } }

    @Test
    fun `nulls are unknown to plain equality, and equal to each other only when asked`(al: BufferAllocator) {
        assertEquals(listOf(listOf(1, 0), listOf(0, 0)), equi(al, listOf(1L, null), listOf(1L, null)))
        assertEquals(listOf(listOf(1, -1), listOf(-1, 1)), equi(al, listOf(1L, null), listOf(1L, null), nullsEqual = true))
    }

    @Test
    fun `numbers compare across widths - two integers as longs, else as doubles`(al: BufferAllocator) {
        assertEquals(listOf(listOf(1, -1)), equi(al, listOf(3), listOf(3L, 4L)))
        assertEquals(listOf(listOf(1, -1)), equi(al, listOf(3L), listOf(3.0, 3.5)))
        assertEquals(listOf(listOf(1, -1)), equi(al, listOf(BigDecimal("3.00")), listOf(3L, 4L)))
        assertEquals(listOf(listOf(0, -1, 1)), order(al, listOf(2.5), listOf(2.5f, 3L, 2L)))
        assertEquals(listOf(listOf(-1)), order(al, listOf(Long.MAX_VALUE - 1), listOf(Long.MAX_VALUE)), "two longs compare exactly")
        assertEquals(listOf(listOf(0)), order(al, listOf(BigDecimal("9007199254740993")), listOf(9007199254740992L)), "a decimal orders as a double")
    }

    @Test
    fun `values of unrelated types are never equal, and have no order`(al: BufferAllocator) {
        assertEquals(listOf(listOf(-1)), equi(al, listOf(1L), listOf("1")))
        assertThrows<Incorrect> { order(al, listOf(1L), listOf("1")) }
    }

    @Test
    fun `strings, keywords and binaries are equal byte for byte, and order unsigned`(al: BufferAllocator) {
        open(al, listOf("foo", "bar")).use { strs ->
            Vector.fromList(al, maybe(KEYWORD), listOf(Keyword.intern("foo"))).use { kws ->
                assertEquals(listOf(listOf(1), listOf(-1)), strs.equiComparator(kws, false, utc).table(strs, kws))
            }
        }

        assertEquals(listOf(listOf(-1, 1)), order(al, listOf("a"), listOf("é", "")))
        assertEquals(listOf(listOf(-1)), order(al, listOf(byteArrayOf(1)), listOf(byteArrayOf(-1))))
    }

    @Test
    fun `nulls order first or last`(al: BufferAllocator) {
        assertEquals(listOf(listOf(0, -1), listOf(1, 0)), order(al, listOf(null, 1L), listOf(null, 1L), nullsFirst = true))
        assertEquals(listOf(listOf(0, 1), listOf(-1, 0)), order(al, listOf(null, 1L), listOf(null, 1L), nullsFirst = false))
    }

    @Test
    fun `timestamps compare across units, and against dates by their local reading in the zone`(al: BufferAllocator) {
        val local = LocalDateTime.of(2020, 1, 1, 0, 0)
        val zone = ZoneId.of("Europe/London")

        Vector.fromList(al, maybe(Scalar(ArrowType.Timestamp(TimeUnit.NANOSECOND, null))), listOf(local, local.plusNanos(1))).use { nanos ->
            Vector.fromList(al, maybe(Scalar(ArrowType.Timestamp(TimeUnit.SECOND, null))), listOf(local)).use { secs ->
                assertEquals(listOf(listOf(0), listOf(1)), nanos.orderComparator(secs, false, utc).table(nanos, secs))
            }
        }

        assertEquals(listOf(listOf(1, -1)), equi(al, listOf(LocalDate.of(2020, 1, 1)), listOf(local, local.plusSeconds(1))))

        val midnightInZone = ZonedDateTime.of(local, ZoneId.of("America/New_York"))
        assertEquals(listOf(listOf(-1)), equi(al, listOf(LocalDate.of(2020, 1, 1)), listOf(midnightInZone), zone = zone))
        assertEquals(listOf(listOf(1)), equi(al, listOf(LocalDate.of(2020, 1, 1)), listOf(midnightInZone), zone = midnightInZone.zone))
    }

    @Test
    fun `times and durations compare across units`(al: BufferAllocator) {
        assertEquals(listOf(listOf(1)), equi(al, listOf(Duration.ofSeconds(1)), listOf(Duration.ofMillis(1000))))
        assertEquals(listOf(listOf(1)), equi(al, listOf(LocalTime.of(1, 0)), listOf(LocalTime.of(1, 0))))
        assertEquals(listOf(listOf(-1)), equi(al, listOf(LocalTime.of(1, 0)), listOf(Duration.ofHours(1))))
    }

    @Test
    fun `intervals compare only within one unit, and month-day intervals only without months`(al: BufferAllocator) {
        Vector.fromList(al, maybe(INTERVAL_YEAR), listOf(Interval(14, 0, 0))).use { ym ->
            Vector.fromList(al, maybe(INTERVAL_MDN), listOf(Interval(14, 0, 0), Interval(0, 1, 0))).use { mdn ->
                assertEquals(listOf(listOf(1, -1)), ym.equiComparator(mdn, false, utc).table(ym, mdn))
                assertThrows<Incorrect> { ym.orderComparator(mdn, false, utc) }

                val cmp = mdn.orderComparator(mdn, false, utc)
                assertEquals(0, cmp.applyAsInt(1, 1))
                assertThrows<Incorrect> { cmp.applyAsInt(0, 1) }
            }
        }
    }

    @Test
    fun `structs equal field by field, in three-valued logic`(al: BufferAllocator) {
        assertEquals(
            listOf(listOf(1, 0, -1)),
            equi(al, listOf(mapOf("a" to 1L, "b" to "x")), listOf(mapOf("a" to 1L, "b" to "x"), mapOf("a" to 1L, "b" to null), mapOf("a" to 2L, "b" to null)))
        )

        assertEquals(listOf(listOf(-1)), equi(al, listOf(mapOf("a" to 1L, "b" to "x")), listOf(mapOf("a" to 1L))), "different keys")
    }

    @Test
    fun `lists equal element by element, in three-valued logic`(al: BufferAllocator) {
        assertEquals(
            listOf(listOf(1, 0, -1, -1)),
            equi(al, listOf(listOf(1L, 2L)), listOf(listOf(1L, 2L), listOf(1L, null), listOf(3L, null), listOf(1L)))
        )
    }

    @Test
    fun `sets have no equality yet`(al: BufferAllocator) {
        Vector.fromList(al, maybe(setTypeOf(I64)), listOf(setOf(1L))).use { sets ->
            assertThrows<Unsupported> { sets.equiComparator(sets, false, utc) }
        }
    }

    @Test
    fun `unions compare leg by leg, and selections and run-end encoding through to the rows beneath`(al: BufferAllocator) {
        open(al, listOf(1L, "a", null, 2.0)).use { union ->
            open(al, listOf(2L, 1L, "a")).use { longs ->
                val sel = IndirectVector(longs, selection(intArrayOf(2, 1, 0)))
                assertEquals(
                    listOf(listOf(-1, 1, -1), listOf(1, -1, -1), listOf(0, 0, 0), listOf(-1, -1, 1)),
                    union.equiComparator(sel, false, utc).table(union, sel)
                )
            }

            RunEndEncodedVector(al, LongVector(al, true)).use { ree ->
                listOf(2L to 2, 1L to 1).forEach { (value, len) -> ree.writeRun(value, len) }
                assertEquals(
                    listOf(listOf(-1, -1, 1), listOf(-1, -1, -1), listOf(0, 0, 0), listOf(1, 1, -1)),
                    union.equiComparator(ree, false, utc).table(union, ree)
                )
            }
        }
    }

    @Test
    fun `a union compares rows of a leg it gained after the comparator was built`(al: BufferAllocator) {
        open(al, listOf(1L, "a")).use { union ->
            open(al, listOf(true)).use { bools ->
                val cmp = union.equiComparator(bools, false, utc)
                (union as DenseUnionVector).writeObject(true)
                assertEquals(listOf(listOf(-1), listOf(-1), listOf(1)), cmp.table(union, bools))
            }
        }
    }
}
