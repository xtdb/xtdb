package xtdb.segment

import xtdb.trie.HashTrie

class TestTrie(override val rootNode: HashTrie.Node<Leaf>?) : HashTrie<TestTrie.Leaf> {
    sealed interface Node : HashTrie.Node<Leaf>

    class Branch(override val path: ByteArray, override val hashChildren: List<HashTrie.Node<Leaf>?>) : Node

    class Leaf(override val path: ByteArray, val pageIdx: Int) : Node {
        override val hashChildren = null
    }

    companion object {
        sealed interface Builder {
            class Branch(val children: List<Builder?>) : Builder {
                override fun build(part: List<Byte>): Node =
                    Branch(
                        part.toByteArray(),
                        children.mapIndexed { idx, child -> child?.build(part + idx.toByte()) }
                    )
            }

            class Leaf(val pageIdx: Int) : Builder {
                override fun build(part: List<Byte>): Node = Leaf(part.toByteArray(), pageIdx)
            }

            fun build(part: List<Byte> = emptyList()): Node
        }

        fun b(vararg children: Builder?) = Builder.Branch(children.toList())
        fun l(pageIdx: Int) = Builder.Leaf(pageIdx)
    }
}