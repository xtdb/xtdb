package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.util.closeOnCatch
import java.nio.file.Path

interface ISegment<L> {
    val trie: HashTrie<L>
    val dataRel: DataRel<L>?

    class Segment<N : HashTrie.Node<N>, L : N>(
        override val trie: HashTrie<L>,
        override val dataRel: DataRel<L>
    ) : ISegment<L>

    class LocalSegment(
        al: BufferAllocator, dataFile: Path, metaFile: Path
    ) : ISegment<ArrowHashTrie.Leaf>, AutoCloseable {

        private val metaRel: Relation
        override val trie: HashTrie<ArrowHashTrie.Leaf>
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