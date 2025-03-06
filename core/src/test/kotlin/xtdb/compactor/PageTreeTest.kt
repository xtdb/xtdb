package xtdb.compactor

import com.carrotsearch.hppc.ByteArrayList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import xtdb.compactor.PageTree.Companion.asTree
import xtdb.compactor.PageTree.Node

class PageTreeTest {

    private fun node(vararg children: PageTree?) =
        Node(children.toMutableList(), children.sumOf { it?.rowCount ?: 0 })

    @Test
    fun `test batch tree construction`() {
        assertNull(emptyList<PageTree.Leaf>().asTree)

        PageTree.Leaf(0, ByteArrayList(), 12).let {
            assertEquals(it, listOf(it).asTree)
        }

        PageTree.Leaf(0, ByteArrayList.from(1), 12).let {
            assertEquals(node(null, it, null, null), listOf(it).asTree)
        }

        val leaf01 = PageTree.Leaf(0, ByteArrayList.from(0, 1), 8)
        val leaf03 = PageTree.Leaf(1, ByteArrayList.from(0, 3), 12)
        val leaf1 = PageTree.Leaf(2, ByteArrayList.from(1), 24)

        assertEquals(
            node(node(null, leaf01, null, leaf03), leaf1, null, null),
            listOf(leaf01, leaf03, leaf1).asTree
        )

        val leaf22 = PageTree.Leaf(3, ByteArrayList.from(2, 2), 2)
        val leaf232 = PageTree.Leaf(4, ByteArrayList.from(2, 3, 2), 10)
        val leaf233 = PageTree.Leaf(5, ByteArrayList.from(2, 3, 3), 11)

        assertEquals(
            node(
                node(null, leaf01, null, leaf03),
                leaf1,
                node(null, null, leaf22, node(null, null, leaf232, leaf233)),
                null
            ),
            listOf(leaf01, leaf03, leaf1, leaf22, leaf232, leaf233).asTree
        )

    }
}