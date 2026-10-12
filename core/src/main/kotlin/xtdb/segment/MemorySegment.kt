package xtdb.segment

import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.metadata.UNBOUND_TEMPORAL_METADATA
import xtdb.segment.Segment.PageMeta.Companion.pageMeta
import xtdb.trie.HashTrie
import xtdb.trie.MemoryHashTrie

class MemorySegment(val trie: MemoryHashTrie, val rel: RelationReader) : Segment<MemoryHashTrie.Leaf> {
    override val part = null
    override val sameSystemTimeResolved = false

    override val schema: Schema get() = rel.schema

    inner class Metadata : Segment.Metadata<MemoryHashTrie.Leaf> {
        override val trie: HashTrie<MemoryHashTrie.Leaf> get() = this@MemorySegment.trie

        override fun page(leaf: MemoryHashTrie.Leaf) =
            pageMeta(
                Segment.Page.page(sameSystemTimeResolved) { rel.select(leaf.mergeSort(this@MemorySegment.trie)) },
                this, leaf, UNBOUND_TEMPORAL_METADATA, recency = Long.MAX_VALUE
            )

        override fun testPage(leaf: MemoryHashTrie.Leaf) = true

        override fun close() = Unit
    }

    override suspend fun openMetadata(): Metadata = Metadata()

    override fun close() = rel.close()
}
