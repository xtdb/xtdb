package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.PageMetadata
import xtdb.trie.HashTrie

interface Segment<L> : AutoCloseable {
    val trie: HashTrie<L>

    val schema: Schema

    val pageMetadata: PageMetadata?

    /**
     * Implementations of DataPage should be able to out-live the related Segment
     */
    interface Page {
        val schema: Schema

        fun testMetadata(): Boolean
        val temporalMetadata: TemporalMetadata

        fun loadDataPage(al: BufferAllocator): RelationReader
    }

    fun page(leaf: L): Page

}