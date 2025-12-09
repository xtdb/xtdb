package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.metadata.UNBOUND_TEMPORAL_METADATA
import xtdb.segment.Segment.PageMeta.Companion.pageMeta
import xtdb.trie.HashTrie
import xtdb.trie.MemoryHashTrie

/**
 * @param rel NOTE: borrows `rel`, doesn't close it.
 */
class MemorySegment(val trie: MemoryHashTrie, val rel: RelationReader) : Segment<MemoryHashTrie.Leaf> {
    override val part = null

    override val schema: Schema get() = rel.schema

    inner class Metadata : Segment.Metadata<MemoryHashTrie.Leaf> {
        override val trie: HashTrie<MemoryHashTrie.Leaf> get() = this@MemorySegment.trie

        override fun page(leaf: MemoryHashTrie.Leaf) =
            pageMeta(this@MemorySegment, this, leaf, UNBOUND_TEMPORAL_METADATA, recency = Long.MAX_VALUE)

        override fun testPage(leaf: MemoryHashTrie.Leaf) = true

        override fun close() = Unit
    }

    override suspend fun openMetadata(): Metadata = Metadata()

    override suspend fun loadDataPage(al: BufferAllocator, leaf: MemoryHashTrie.Leaf) = rel.select(leaf.mergeSort(trie))

    override fun close() = Unit
}