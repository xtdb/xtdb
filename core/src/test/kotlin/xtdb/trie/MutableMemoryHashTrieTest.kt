package xtdb.trie

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.IntVector
import kotlin.random.Random

class MutableMemoryHashTrieTest {
    private lateinit var allocator: BufferAllocator
    private lateinit var hashReader: IntVector
    private lateinit var trie: MutableMemoryHashTrie

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        hashReader = IntVector.open(allocator, "hash", true)
        trie = MutableMemoryHashTrie.builder(hashReader).build()
    }

    @AfterEach
    fun tearDown() {
        hashReader.close()
        allocator.close()
    }

    fun MutableMemoryHashTrie.findCandidates(hashCode: Int): IntArrayList {
        val res = IntArrayList()
        forEachMatch(hashCode) { res.add(it) }
        return res
    }

    @Test
    fun `test add of MutableMemoryHashTrie`() {
        val r = Random(42)
        val testSize = 10000
        val hashes = List(testSize) { r.nextInt() }
        hashes.forEachIndexed({ index, hash ->
            hashReader.writeInt(hash)
            trie += index
        })

        trie.sortData()

        for (i in 0 until testSize) {
            val hash = hashes[i]
            val idx = trie.findValue(hash, { _ -> 1 }, false)
            assertTrue(idx >= 0, "Hash $hash at position $i not found in trie")
            assertEquals(hashes[i], hashes[idx])
            val idxs = trie.findCandidates(hashes[i])
            assertTrue(idxs.size() > 0, "Candidates for hash $i not found in trie")
            assertTrue(
                idxs.toArray().all { hashes[it] == hashes[i] },
                "All candidates for hash $i should match the original hash"
            )
        }
    }

    @Test
    fun `test addIfNotPresent of MutableMemoryHashTrie`() {
        val r = Random(42)
        val testSize = 100000
        val hashes = List(testSize) { r.nextInt() }
        hashes.forEachIndexed({ index, hash ->
            hashReader.writeInt(hash)
            trie += index
        })


        hashes.forEachIndexed({ index, hash ->
            val newIndex = hashes.size + index
            assertTrue(
                trie.addIfNotPresent(
                    hash,
                    newIndex,
                    { _ -> 1 },
                    { error("Should not happen!") }).first != newIndex, "Hash $hash should already be present"
            )
        })

        val newHashes = List(testSize) { Random(42).nextInt() }.filter { it !in hashes }

        newHashes.forEachIndexed({ index, hash ->
            val newIndex = hashes.size + index
            val (addedIdx, _) = trie.addIfNotPresent(
                hash,
                index + hashes.size,
                { _ -> 1 },
                { hashReader.writeInt(hash) })
            assertEquals(newIndex == addedIdx, "addedIndex should be newIndex")
        })
    }
}