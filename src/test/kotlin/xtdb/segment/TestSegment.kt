package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema

class TestSegment(val name: String, val trie: TestTrie) : Segment<TestTrie.Leaf> {
    override val part = null
    override val schema: Schema get() = error("schema")

    class Page(val name: String, val pageIdx: Int) : Segment.Page<TestTrie.Leaf>, Segment.PageMeta<TestTrie.Leaf> {
        override suspend fun loadDataPage(al: BufferAllocator) = error("loadDataPage")
        override val page: Segment.Page<TestTrie.Leaf> get() = this
        override val temporalMetadata get() = error("temporalMetadata")
        override fun testMetadata() = error("testMetadata")
    }

    override suspend fun openMetadata() = object : Segment.Metadata<TestTrie.Leaf> {
        override val trie = this@TestSegment.trie

        override fun page(leaf: TestTrie.Leaf) = Page(name, leaf.pageIdx)

        override fun testPage(leaf: TestTrie.Leaf) = error("testPage")
        override fun close() = Unit
    }

    override suspend fun loadDataPage(al: BufferAllocator, leaf: TestTrie.Leaf) = error("loadDataPage")
    override fun close() = Unit

    companion object {
        fun seg(name: String, trie: TestTrie.Companion.Builder) =
            TestSegment(name, TestTrie(trie.build()))
    }
}