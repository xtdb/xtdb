package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.log.proto.TemporalMetadata
import xtdb.segment.Segment.Page.Companion.page
import xtdb.trie.HashTrie

interface Segment<L> : AutoCloseable {
    val schema: Schema

    fun openMetadata(): Metadata<L>

    interface Metadata<L> : AutoCloseable {
        val trie: HashTrie<L>

        fun page(leaf: L): PageMeta<L>
        fun testPage(leaf: L): Boolean
    }

    interface Page<L> {

        fun loadDataPage(al: BufferAllocator): RelationReader

        companion object {
            fun <L> page(segment: Segment<L>, leaf: L) = object : Page<L> {
                override fun loadDataPage(al: BufferAllocator) = segment.loadDataPage(al, leaf)
            }
        }
    }

    interface PageMeta<L> {
        val page: Page<L>
        val temporalMetadata: TemporalMetadata

        fun testMetadata(): Boolean

        companion object {
            fun <L> pageMeta(seg: Segment<L>, meta: Metadata<L>, leaf: L, temporalMetadata: TemporalMetadata) =
                object : PageMeta<L> {
                    override val page get() = page(seg, leaf)
                    override val temporalMetadata get() = temporalMetadata

                    private var testMetadata: Boolean? = null

                    override fun testMetadata() =
                        testMetadata ?: meta.testPage(leaf).also { testMetadata = it }
                }
        }
    }

    fun loadDataPage(al: BufferAllocator, leaf: L): RelationReader
}