package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import xtdb.storage.BufferPool
import xtdb.arrow.RelationReader
import xtdb.log.proto.TrieMetadata
import xtdb.table.TrieFiles

class LiveTrieWriter(
    private val al: BufferAllocator, private val bp: BufferPool,
    private val calculateBlooms: Boolean
) {
    suspend fun writeLiveTrie(files: TrieFiles, trieKey: TrieKey, trie: MemoryHashTrie, dataRel: RelationReader): Pair<FileSize, TrieMetadata> =
        DataFileWriter(al, bp, files, trieKey, dataRel.schema).use { dataFileWriter ->
            MetadataFileWriter(al, bp, files, trieKey, dataFileWriter.dataRel, calculateBlooms)
                .use { metaFileWriter ->
                    val copier = dataRel.rowCopier(dataFileWriter.dataRel)

                    fun MemoryHashTrie.Node.writeNode(): Int =
                        when (this) {
                            is MemoryHashTrie.Branch ->
                                metaFileWriter.writeIidBranch(
                                    IntArray(hashChildren.size) { idx -> hashChildren[idx]?.writeNode() ?: -1 }
                                )

                            is MemoryHashTrie.Leaf -> {
                                copier.copyRows(data)
                                metaFileWriter.writeLeaf().also { dataFileWriter.writePage() }
                            }
                        }

                    trie.compactLogs().rootNode.writeNode()

                    val trieMetadata = metaFileWriter.end()
                    Pair(dataFileWriter.end(), trieMetadata)
                }
        }
}
