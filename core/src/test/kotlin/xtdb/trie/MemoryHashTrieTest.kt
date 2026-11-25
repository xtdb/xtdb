package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.IntVector
import kotlin.random.Random

class MemoryHashTrieTest {
    private lateinit var allocator: BufferAllocator
    private lateinit var iidReader: IntVector

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        iidReader = IntVector.open(allocator, true)
    }

    @AfterEach
    fun tearDown() {
        iidReader.close()
        allocator.close()
    }

    private fun emptyTrie(logLimit: Int = 8, pageLimit: Int = 32): MemoryHashTrie =
        MemoryHashTrie.builder(iidReader)
            .setLogLimit(logLimit)
            .setPageLimit(pageLimit)
            .build()

    private fun writeRandomIids(count: Int, seed: Long = 42L) {
        val r = Random(seed)
        for (i in 0 until count) iidReader.writeInt(r.nextInt())
    }

    /**
     * Indices reachable from any leaf, after compaction. Tree shape may vary
     * legitimately by insertion order; the contained-index multiset is the
     * invariant that addAll/addRange must preserve.
     */
    private fun MemoryHashTrie.indexSet(): Set<Int> =
        compactLogs().leaves.flatMap { it.data.toList() }.toSet()

    @Test
    fun `addRange covers same indices as folded plus, within log limit`() {
        val n = 6
        writeRandomIids(n)
        val byRange = emptyTrie().addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie()) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `addRange covers same indices as folded plus, across log compactions`() {
        // logLimit = 8, pageLimit = 32: 20 entries forces at least one compaction
        // but stays under the page split threshold.
        val n = 20
        writeRandomIids(n)
        val byRange = emptyTrie().addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie()) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `addRange covers same indices as folded plus, across page splits`() {
        val n = 200
        writeRandomIids(n)
        val byRange = emptyTrie().addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie()) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `addRange in chunks covers same indices as one big addRange`() {
        val n = 150
        writeRandomIids(n)
        val byChunks = emptyTrie()
            .addRange(0, 7)
            .addRange(7, 50)
            .addRange(57, n - 57)
        val byOne = emptyTrie().addRange(0, n)
        assertEquals(byOne.indexSet(), byChunks.indexSet())
        assertEquals((0 until n).toSet(), byOne.indexSet())
    }

    @Test
    fun `addRange exactly at log limit triggers single compaction`() {
        // Pins the boundary case in `Leaf.addAll` where combinedLogCount == logLimit:
        // append in place, then run one-pass compactLogs (matching single-add at
        // the same boundary).
        val n = 8
        writeRandomIids(n)
        val byRange = emptyTrie(logLimit = 8, pageLimit = 32).addRange(0, n)
        val byFold = (0 until n).fold(emptyTrie(logLimit = 8, pageLimit = 32)) { t, i -> t + i }
        assertEquals(byFold.indexSet(), byRange.indexSet())
        assertEquals((0 until n).toSet(), byRange.indexSet())
    }

    @Test
    fun `addRange with zero count is identity`() {
        val trie = emptyTrie()
        assertSame(trie, trie.addRange(0, 0))
    }

    @Test
    fun `addAll empty is identity`() {
        val trie = emptyTrie()
        assertSame(trie, trie.addAll(IntArray(0)))
    }
}
