package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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

    private fun openIids(vararg iids: Int) =
        FixedSizeBinaryVector(al, "_iid", false, 16)
            .also { vec -> iids.forEach { b -> vec.writeObject(ByteArray(16) { b.toByte() }) } }

    private fun LongLongVectorReader.runEnds(limit: Int = valueCount): List<Int> =
        generateSequence(0) { idx -> equalRunEnd(idx, limit).takeIf { it < limit } }
            .map { equalRunEnd(it, limit) }
            .toList()

    @Test
    fun `each run ends where the next value starts`() {
        openIids(1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4).use { iids ->
            assertEquals(listOf(1, 3, 13, 14), iids.runEnds())
        }
    }

    @Test
    fun `a run starting part-way through ends with it`() {
        openIids(1, 1, 1, 1, 1, 2).use { iids ->
            assertEquals(5, iids.equalRunEnd(3, iids.valueCount))
        }
    }

    @Test
    fun `a run is cut short at the limit`() {
        openIids(1, 1, 1, 1, 1, 2).use { iids ->
            assertEquals(3, iids.equalRunEnd(0, 3))
            assertEquals(listOf(3), iids.runEnds(limit = 3))
        }
    }
}
