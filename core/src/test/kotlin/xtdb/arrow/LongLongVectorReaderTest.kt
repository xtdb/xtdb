package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class LongLongVectorReaderTest {

    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    enum class Layout {
        PLAIN, RUNS, SINGLE_ROW_RUNS;

        fun open(al: BufferAllocator, iids: List<Int>): Vector = when (this) {
            PLAIN -> FixedSizeBinaryVector(al, "_iid", false, 16)
                .also { vec -> iids.forEach { vec.writeObject(ByteArray(16) { _ -> it.toByte() }) } }

            RUNS -> RunEndEncodedVector(al, "_iid", FixedSizeBinaryVector(al, REE_VALUES_NAME, false, 16))
                .also { vec -> iids.forEach { vec.appendLongLong(it.toLong() * 0x0101010101010101L, it.toLong() * 0x0101010101010101L) } }

            SINGLE_ROW_RUNS -> RunEndEncodedVector(al, "_iid", FixedSizeBinaryVector(al, REE_VALUES_NAME, false, 16))
                .also { vec -> iids.forEach { vec.writeRun(ByteArray(16) { _ -> it.toByte() }) } }
        }
    }

    private fun LongLongVectorReader.runEnds(limit: Int = valueCount): List<Int> =
        generateSequence(0) { idx -> equalRunEnd(idx, limit).takeIf { it < limit } }
            .map { equalRunEnd(it, limit) }
            .toList()

    @ParameterizedTest
    @EnumSource(Layout::class)
    fun `each run ends where the next value starts`(layout: Layout) {
        layout.open(al, listOf(1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4)).use { iids ->
            assertEquals(listOf(1, 3, 13, 14), (iids as LongLongVectorReader).runEnds())
        }
    }

    @ParameterizedTest
    @EnumSource(Layout::class)
    fun `a run starting part-way through ends with it`(layout: Layout) {
        layout.open(al, listOf(1, 1, 1, 1, 1, 2)).use { iids ->
            assertEquals(5, (iids as LongLongVectorReader).equalRunEnd(3, iids.valueCount))
        }
    }

    @ParameterizedTest
    @EnumSource(Layout::class)
    fun `a run is cut short at the limit`(layout: Layout) {
        layout.open(al, listOf(1, 1, 1, 1, 1, 2)).use { iids ->
            iids as LongLongVectorReader
            assertEquals(3, iids.equalRunEnd(0, 3))
            assertEquals(listOf(3), iids.runEnds(limit = 3))
        }
    }

    @ParameterizedTest
    @EnumSource(Layout::class)
    fun `every row before a stored run's end holds the value it started with`(layout: Layout) {
        val values = listOf(1, 1, 2, 3, 3, 3, 4)

        layout.open(al, values).use { iids ->
            iids as LongLongVectorReader
            repeat(iids.valueCount) { idx ->
                val end = iids.storedRunEnd(idx)
                assert(end > idx && end <= iids.valueCount) { "stored run end $end out of range for $idx" }
                (idx..<end).forEach { assertEquals(values[idx], values[it], "row $it within $idx's stored run") }
            }
        }
    }
}
