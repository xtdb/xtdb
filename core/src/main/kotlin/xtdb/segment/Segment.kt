package xtdb.segment

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.log.proto.TemporalMetadata
import xtdb.segment.Segment.Page.Companion.page
import xtdb.trie.HashTrie
import xtdb.trie.RecencyMicros

interface Segment<L> : AutoCloseable {
    val part: ByteArray?

    val schema: Schema

    /**
     * Whether events sharing a `_system_from` have been resolved against each other in this segment's pages.
     * L0 files and the live index hold them raw, in live-index order; see #4303 for what resolution does to them.
     */
    val sameSystemTimeResolved: Boolean

    fun openMetadataSync(): Metadata<L> = runBlocking { openMetadata() }
    suspend fun openMetadata(): Metadata<L>

    interface Metadata<L> : AutoCloseable {
        val trie: HashTrie<L>

        fun page(leaf: L): PageMeta<L>
        fun testPage(leaf: L): Boolean
    }

    interface Page<L> {
        val sameSystemTimeResolved: Boolean

        suspend fun loadDataPage(al: BufferAllocator): RelationReader

        /**
         * Called once the last [MergeTask] that includes this page has finished with it;
         * the relation [loadDataPage] returned mustn't be used afterwards.
         */
        fun release()

        companion object {
            /**
             * A page that doesn't own what it loads, so [release] does nothing.
             */
            fun <L> page(sameSystemTimeResolved: Boolean, load: suspend (BufferAllocator) -> RelationReader) =
                object : Page<L> {
                    override val sameSystemTimeResolved = sameSystemTimeResolved
                    override suspend fun loadDataPage(al: BufferAllocator) = load(al)
                    override fun release() = Unit
                }
        }
    }

    interface PageMeta<L> {
        /**
         * The same page for the same leaf, for the lifetime of the segment —
         * the merge planner releases a page after the last task that includes it.
         */
        val page: Page<L>
        val temporalMetadata: TemporalMetadata
        val recency: RecencyMicros

        fun testMetadata(): Boolean

        companion object {
            fun <L> pageMeta(page: Page<L>, meta: Metadata<L>, leaf: L, temporalMetadata: TemporalMetadata, recency: RecencyMicros) =
                object : PageMeta<L> {
                    override val page get() = page
                    override val temporalMetadata get() = temporalMetadata
                    override val recency get() = recency

                    private var testMetadata: Boolean? = null

                    override fun testMetadata() =
                        testMetadata ?: meta.testPage(leaf).also { testMetadata = it }
                }
        }
    }
}