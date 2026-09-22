package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation

internal class ArrowHashTrieTest {

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
    fun `a trie with no nodes has no root`() {
        Relation(allocator, MetadataFileWriter.metaRelSchema).use { rel ->
            assertNull(ArrowHashTrie(rel["nodes"]).rootNode)
        }
    }
}
