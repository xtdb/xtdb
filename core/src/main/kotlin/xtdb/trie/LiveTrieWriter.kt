package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import xtdb.storage.BufferPool
import xtdb.arrow.RelationReader
import xtdb.table.TableRef

class LiveTrieWriter(
    private val al: BufferAllocator, private val bp: BufferPool,
    private val calculateBlooms: Boolean
) {
    fun writeLiveTrie(table: TableRef, trieKey: TrieKey, trie: MemoryHashTrie, dataRel: RelationReader): FileSize =
        DataFileWriter(al, bp, table, trieKey, dataRel.schema).use { dataFileWriter ->
            MetadataFileWriter(al, bp, table, trieKey, dataFileWriter.dataRel, calculateBlooms, false)
                .use { metaFileWriter ->
                    val copier = dataFileWriter.dataRel.rowCopier(dataRel)

                    fun MemoryHashTrie.Node.writeNode(): Int =
                        when (this) {
                            is MemoryHashTrie.Branch ->
                                metaFileWriter.writeIidBranch(
                                    IntArray(hashChildren.size) { idx -> hashChildren[idx]?.writeNode() ?: -1 }
                                )

                            is MemoryHashTrie.Leaf -> {
                                data.forEach { idx -> copier.copyRow(idx) }
                                metaFileWriter.writeLeaf().also { dataFileWriter.writePage() }
                            }
                        }

                    trie.compactLogs().rootNode.writeNode()

                    metaFileWriter.end()
                    dataFileWriter.end()
                }
        }
}
