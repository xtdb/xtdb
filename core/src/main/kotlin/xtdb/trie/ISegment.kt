package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.util.closeOnCatch
import java.nio.file.Path

interface ISegment<N : HashTrie.Node<N>, L : N> {
    val trie: HashTrie<N, L>
    val dataRel: DataRel<L>?

    class Segment<N : HashTrie.Node<N>, L : N>(
        override val trie: HashTrie<N, L>,
        override val dataRel: DataRel<L>
    ) : ISegment<N, L>

    class LocalSegment(
        al: BufferAllocator, dataFile: Path, metaFile: Path
    ) : ISegment<ArrowHashTrie.Node, ArrowHashTrie.Leaf>, AutoCloseable {

        private val metaRel: Relation
        override val trie: HashTrie<ArrowHashTrie.Node, ArrowHashTrie.Leaf>
        override val dataRel = DataRel.LocalFile(al, dataFile)

        init {
            Relation.loader(al, metaFile).use { loader ->
                loader.loadPage(0, al).closeOnCatch { metaRel ->
                    this.metaRel = metaRel
                    trie = ArrowHashTrie(metaRel["nodes"])
                }
            }
        }

        override fun close() {
            dataRel.close()
            metaRel.close()
        }
    }
}