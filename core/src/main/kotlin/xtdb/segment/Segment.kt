package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.log.proto.TemporalMetadata
import xtdb.metadata.PageMetadata
import xtdb.operator.scan.Metadata
import xtdb.trie.HashTrie

interface Segment<L> : AutoCloseable {
    val trie: HashTrie<L>

    val schema: Schema

    val pageMetadata: PageMetadata?

    /**
     * Implementations of DataPage should be able to out-live the related Segment
     */
    interface Page : Metadata {
        val schema: Schema

        override fun testMetadata(): Boolean
        override val temporalMetadata: TemporalMetadata

        fun openDataPage(al: BufferAllocator): RelationReader
    }

    fun page(leaf: L): Page

}