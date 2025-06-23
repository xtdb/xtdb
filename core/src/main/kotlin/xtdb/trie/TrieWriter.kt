package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.compactor.PageTree
import xtdb.log.proto.TrieMetadata
import xtdb.trie.HashTrie.Companion.LEVEL_WIDTH

private typealias Selection = IntArray

class TrieWriter(
    private val al: BufferAllocator, private val bp: BufferPool,
    private val calculateBlooms: Boolean
) {

    fun writeLiveTrie(tableName: TableName, trieKey: TrieKey, trie: MemoryHashTrie, dataRel: RelationReader): FileSize =
        DataFileWriter(al, bp, tableName, trieKey, dataRel.schema).use { dataFileWriter ->
            MetadataFileWriter(al, bp, tableName, trieKey, dataFileWriter.dataRel, calculateBlooms, false)
                .use { metaFileWriter ->
                    val copier = dataFileWriter.dataRel.rowCopier(dataRel)

                    fun MemoryHashTrie.Node.writeNode(): Int =
                        when (this) {
                            is MemoryHashTrie.Branch ->
                                metaFileWriter.writeIidBranch(
                                    IntArray(iidChildren.size) { idx -> iidChildren[idx]?.writeNode() ?: -1 }
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

    companion object {
        internal fun Selection.partitionSlices(partIdxs: IntArray) =
            Array(LEVEL_WIDTH) { partition ->
                val cur = partIdxs[partition]
                val nxt = if (partition == partIdxs.lastIndex) size else partIdxs[partition + 1]

                if (cur == nxt) null else sliceArray(cur..<nxt)
            }

        internal fun Selection.iidPartitions(iidReader: VectorReader, level: Int): Array<Selection?> {
            val iidPtr = ArrowBufPointer()

            // for each partition, find the starting index in the selection
            val partIdxs = IntArray(LEVEL_WIDTH) { partition ->
                var left = 0
                var right = size
                var mid: Int
                while (left < right) {
                    mid = (left + right) / 2

                    val bucket = HashTrie.bucketFor(iidReader.getPointer(this[mid], iidPtr), level)

                    if (bucket < partition) left = mid + 1 else right = mid
                }

                left
            }

            // slice the selection array for each partition
            return partitionSlices(partIdxs)
        }
    }

    fun writePageTree(
        tableName: TableName, trieKey: TrieKey,
        loader: Relation.Loader, pageTree: PageTree?,
        pageSize: Int
    ): Pair<FileSize, TrieMetadata?> =
        DataFileWriter(al, bp, tableName, trieKey, loader.schema).use { dataFileWriter ->
            val dataRel = dataFileWriter.dataRel

            val startPtr = ArrowBufPointer()
            val endPtr = ArrowBufPointer()

            MetadataFileWriter(al, bp, tableName, trieKey, dataFileWriter.dataRel, calculateBlooms, true)
                .use { metaFileWriter ->
                    Relation.open(al, loader.schema).use { inRel ->
                        val iidReader = inRel["_iid"]

                        fun Selection.soloIid(): Boolean =
                            iidReader.getPointer(first(), startPtr) == iidReader.getPointer(last(), endPtr)

                        val rowCopier = dataRel.rowCopier(inRel)

                        fun writeLeaf(): RowIndex = metaFileWriter.writeLeaf().also { dataFileWriter.writePage() }

                        fun writeSubtree(depth: Int, sel: Selection): RowIndex =
                            when {
                                Thread.interrupted() -> throw InterruptedException()

                                sel.isEmpty() -> metaFileWriter.writeNull()

                                sel.size <= pageSize || depth >= 64 || sel.soloIid() -> {
                                    for (idx in sel) rowCopier.copyRow(idx)
                                    writeLeaf()
                                }

                                else -> metaFileWriter.writeIidBranch(
                                    sel.iidPartitions(iidReader, depth)
                                        .map { if (it != null) writeSubtree(depth + 1, it) else -1 }
                                        .toIntArray())
                            }

                        fun PageTree?.writeSubtree(depth: Int): RowIndex {
                            if (Thread.interrupted()) throw InterruptedException()

                            return when (this) {
                                null -> metaFileWriter.writeNull()

                                is PageTree.Leaf -> {
                                    if (rowCount > pageSize) {
                                        // split large leaves
                                        loader.loadPage(pageIdx, inRel)
                                        writeSubtree(depth, IntArray(inRel.rowCount) { idx -> idx })
                                    } else {
                                        loader.loadPage(pageIdx, dataRel)
                                        writeLeaf()
                                    }
                                }

                                is PageTree.Node -> {
                                    if (rowCount > pageSize) {
                                        val idxs = IntArray(children.size) {
                                            children[it]?.writeSubtree(depth + 1) ?: -1
                                        }
                                        metaFileWriter.writeIidBranch(idxs)
                                    } else {
                                        // combine small leaves
                                        for (leaf in leaves) {
                                            loader.loadPage(leaf.pageIdx, inRel)
                                            dataRel.append(inRel)
                                        }

                                        writeLeaf()
                                    }
                                }
                            }
                        }

                        pageTree.writeSubtree(0)

                        Pair(dataFileWriter.end(), metaFileWriter.end())
                    }
                }
        }
}
