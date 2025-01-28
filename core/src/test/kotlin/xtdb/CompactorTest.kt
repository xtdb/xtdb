package xtdb

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.compactor.Compactor.Companion.iidPartitions
import xtdb.compactor.Compactor.Companion.partitionSlices
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.VectorReader
import java.nio.ByteBuffer
import java.util.*

internal fun UUID.toBytes(): ByteArray =
    ByteBuffer.allocate(16).run {
        putLong(mostSignificantBits)
        putLong(leastSignificantBits)
    }.array()

internal fun <V> usingIidReader(allocator: BufferAllocator, iids: List<UUID>, f: (VectorReader) -> V): V =
    FixedSizeBinaryVector(allocator, "_iid", false, 16).use { vec ->
        iids.forEach { vec.writeBytes(ByteBuffer.wrap(it.toBytes())) }

        vec.valueCount = iids.size

        f(vec)
    }

internal class CompactorTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun `test slicing selection`() {
        val selection = IntArray(10) { idx -> idx }
        assertArrayEquals(
            arrayOf(intArrayOf(0, 1, 2, 3), null, intArrayOf(4, 5, 6, 7, 8, 9), null),
            selection.partitionSlices(intArrayOf(0, 4, 4, 10))
        )
    }

    @Test
    fun `test partitioning selection`() {
        usingIidReader(allocator, emptyList()) { iidReader ->
            assertArrayEquals(
                arrayOfNulls(4),
                IntArray(0) { it }.iidPartitions(iidReader, 0),
                "empty selection"
            )
        }

        /*
         split to two levels, this looks like:
         `[[[0] nil nil [3]], [[4] [5] nil [7 7]], nil, [nil [d d] [e] nil]]`
         or, in indices
         `[[[0] nil nil [1]], [[2] [3] nil [4 5]], nil, [nil [6 7] [8] nil]]`
         */
        val iids = listOf(
            UUID.fromString("05000000-0000-0000-0000-000000000000"),
            UUID.fromString("35000000-0000-0000-0000-000000000000"),
            UUID.fromString("45000000-0000-0000-0000-000000000000"),
            UUID.fromString("55000000-0000-0000-0000-000000000000"),
            UUID.fromString("75000000-0000-0000-0000-000000000000"),
            UUID.fromString("75000000-0000-0000-0000-000000000000"),
            UUID.fromString("d5000000-0000-0000-0000-000000000000"),
            UUID.fromString("d5000000-0000-0000-0000-000000000000"),
            UUID.fromString("e5000000-0000-0000-0000-000000000000"),
        )

        usingIidReader(allocator, iids) { iidReader ->
            val l0Selection = IntArray(9) { it }.iidPartitions(iidReader, 0)

            assertArrayEquals(
                arrayOf(intArrayOf(0, 1), intArrayOf(2, 3, 4, 5), null, intArrayOf(6, 7, 8)),
                l0Selection,
                "top level"
            )

            assertArrayEquals(
                arrayOf(intArrayOf(2), intArrayOf(3), null, intArrayOf(4, 5)),
                l0Selection[1]!!.iidPartitions(iidReader, 1),
                "second level, idx 1"
            )

            assertArrayEquals(
                arrayOf(null, intArrayOf(6, 7), intArrayOf(8), null),
                l0Selection[3]!!.iidPartitions(iidReader, 1),
                "second level, idx 3"
            )

            assertArrayEquals(
                arrayOf(null, IntArray(9) { it }, null, null),
                IntArray(9) { it }.iidPartitions(iidReader, 3),
                "nothing in first or last partitions"
            )
        }
    }
}
