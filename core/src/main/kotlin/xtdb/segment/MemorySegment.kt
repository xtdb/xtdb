package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.PageMetadata
import xtdb.metadata.UNBOUND_TEMPORAL_METADATA
import xtdb.trie.MemoryHashTrie

/**
 * @param rel NOTE: borrows `rel`, doesn't close it.
 */
class MemorySegment(override val trie: MemoryHashTrie, val rel: RelationReader) : Segment<MemoryHashTrie.Leaf> {
    override val schema: Schema get() = rel.schema

    override val pageMetadata: PageMetadata? = null

    // this one is the exception to the rule - nothing in the Memory class that needs closing,
    // so DataPage can be an inner object.
    inner class Page(val leaf: MemoryHashTrie.Leaf) : Segment.Page {
        override fun testMetadata() = true
        override val temporalMetadata: TemporalMetadata get() = UNBOUND_TEMPORAL_METADATA

        override val schema: Schema get() = this@MemorySegment.schema

        override fun loadDataPage(al: BufferAllocator) = rel.select(leaf.mergeSort(trie))
    }

    override fun page(leaf: MemoryHashTrie.Leaf) = Page(leaf)

    override fun close() = Unit
}