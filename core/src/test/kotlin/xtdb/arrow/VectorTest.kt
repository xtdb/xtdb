package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.TSTZ_RANGE
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.setTypeOf
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.test.AllocatorResolver
import xtdb.types.ZonedDateTimeRange
import java.time.ZonedDateTime

@ExtendWith(AllocatorResolver::class)
class VectorTest {

    @Test
    fun `doesn't promote if source is a null-vec #4675`(al: BufferAllocator) {
        LongVector(al, true).use { vec ->
            val maybePromoted = vec.maybePromote(al, NULL_TYPE, true)
            assertSame(vec, maybePromoted)
        }
    }

    companion object {
        private val zdt = ZonedDateTime.parse("2020-01-01T00:00Z[UTC]")

        @JvmStatic
        fun copiedValues(): List<Arguments> = listOf(
            Arguments.of(listTypeOf(I64), listOf(listOf(1L, 2L), emptyList<Long>(), listOf(3L))),
            Arguments.of(setTypeOf(I64), listOf(setOf(1L, 2L), emptySet<Long>())),
            Arguments.of(TSTZ_RANGE, listOf(ZonedDateTimeRange(zdt, null), ZonedDateTimeRange(null, zdt))),
            Arguments.of(structOf("a" to I64, "b" to UTF8), listOf(mapOf("a" to 1L, "b" to "x"), mapOf("a" to 2L, "b" to "y"))),
        )
    }

    @ParameterizedTest
    @MethodSource("copiedValues")
    fun `writeValue copies each row from another vector's value reader`(type: VectorType, values: List<Any>, al: BufferAllocator) {
        al.openVector(type).use { src ->
            values.forEach { src.writeObject(it) }

            al.openVector(type).use { dest ->
                val rdr = src.valueReader()
                for (idx in values.indices) {
                    rdr.pos = idx
                    dest.writeValue(rdr)
                }

                assertEquals(values, dest.asList)
            }
        }
    }
}
