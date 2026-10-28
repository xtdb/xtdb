package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.Vector
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorType.Companion.I64
import xtdb.trie.PageTrieWriter.Companion.iidPartitions
import xtdb.trie.PageTrieWriter.Companion.partitionSlices
import xtdb.trie.PageTrieWriter.Companion.recencyParts
import xtdb.trie.PageTrieWriter.Companion.recencyTurn
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

internal class PageTrieWriterTest {

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

    @Test
    fun `recency parts keep each part's rows in selection order, with the range they actually hold`() {
        Vector.fromList(allocator, "_recency", I64, listOf(150L, 100L, 199L, 160L, 101L)).use { recencyReader ->
            val parts = IntArray(5) { it }.recencyParts(recencyReader, RecencyRange(100, 199), 4)

            assertEquals(
                listOf(
                    Triple(0, RecencyRange(100, 101), listOf(1, 4)),
                    Triple(2, RecencyRange(150, 160), listOf(0, 3)),
                    Triple(3, RecencyRange(199, 199), listOf(2)),
                ),
                parts.map { Triple(it.idx, it.range, it.sel.toList()) },
                "the second part is empty"
            )
        }
    }

    @Test
    fun `below the pinned IID levels, IID and recency alternate, IID first`() {
        fun schedule(pinnedDepth: Int): String {
            var depth = 0
            var levelsUsed = 0

            return buildString {
                repeat(6) {
                    if (recencyTurn(depth, levelsUsed, pinnedDepth)) {
                        append('R'); levelsUsed++
                    } else {
                        append('I'); depth++
                    }
                }
            }
        }

        assertEquals("IRIRIR", schedule(0))
        assertEquals("IRIRIR", schedule(1))
        assertEquals("IIRIRI", schedule(2))
    }
}
