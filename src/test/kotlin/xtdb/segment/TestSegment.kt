package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.trie.RecencyMicros

class TestSegment(val name: String, val trie: TestTrie, private val releaseLog: MutableList<Page>? = null) : Segment<TestTrie.Leaf> {
    override val part = null
    override val sameSystemTimeResolved = true
    override val schema: Schema get() = error("schema")

    inner class Page(val name: String, val pageIdx: Int) : Segment.Page<TestTrie.Leaf>, Segment.PageMeta<TestTrie.Leaf> {
        override val sameSystemTimeResolved = true
        override suspend fun loadDataPage(al: BufferAllocator) = error("loadDataPage")
        override fun release() { releaseLog?.add(this) }
        override val page: Segment.Page<TestTrie.Leaf> get() = this
        override val temporalMetadata get() = error("temporalMetadata")
        override val recency: RecencyMicros get() = Long.MAX_VALUE
        override fun testMetadata() = error("testMetadata")
    }

    private val pages = HashMap<Int, Page>()

    override suspend fun openMetadata() = object : Segment.Metadata<TestTrie.Leaf> {
        override val trie = this@TestSegment.trie

        override fun page(leaf: TestTrie.Leaf) = pages.getOrPut(leaf.pageIdx) { Page(name, leaf.pageIdx) }

        override fun testPage(leaf: TestTrie.Leaf) = error("testPage")
        override fun close() = Unit
    }

    override fun close() = Unit

    companion object {
        fun seg(name: String, trie: TestTrie.Companion.Builder, releaseLog: MutableList<Page>? = null) =
            TestSegment(name, TestTrie(trie.build()), releaseLog)
    }
}
