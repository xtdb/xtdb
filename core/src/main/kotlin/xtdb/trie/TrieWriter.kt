package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.ArrowWriter
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.VectorReader
import xtdb.metadata.PageMetadataWriter
import xtdb.trie.HashTrie.Companion.LEVEL_WIDTH
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.Trie.metaRelSchema
import xtdb.util.requiringResolve

private typealias Selection = IntArray

class TrieWriter(
    private val allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    private val writeContentMetadata: Boolean
) {

    private inner class OpenWriter(
        private val tableName: TableName, private val trieKey: TrieKey, dataSchema: Schema
    ) : AutoCloseable {
        val dataRel: Relation = Relation(allocator, dataSchema)

        private val dataFileWriter: ArrowWriter =
            runCatching { bufferPool.openArrowWriter(tableName.dataFilePath(trieKey), dataRel) }
                .onFailure { dataRel.close() }
                .getOrThrow()

        private val metaRel: Relation =
            runCatching { Relation(allocator, metaRelSchema) }
                .onFailure { dataRel.close(); dataFileWriter.close() }
                .getOrThrow()

        private val nodeWtr = metaRel["nodes"]!!
        private val nullBranchWtr = nodeWtr.legWriter("nil")

        private val iidBranchWtr = nodeWtr.legWriter("branch-iid")
        private val iidBranchElWtr = iidBranchWtr.elementWriter

        private val leafWtr = nodeWtr.legWriter("leaf")
        private val pageIdxWtr = leafWtr.keyWriter("data-page-idx")

        private val pageMetaWriter =
            requiringResolve("xtdb.metadata/->page-meta-wtr")
                .invoke(leafWtr.keyWriter("columns"))
                .let { it as PageMetadataWriter }

        private var pageIdx = 0

        fun writeNull(): RowIndex {
            val pos = nodeWtr.valueCount
            nullBranchWtr.writeNull()
            return pos
        }

        fun writeLeaf(): RowIndex {
            val putReader = dataRel["op"]!!.legReader("put")
            val metaPos = nodeWtr.valueCount

            val temporalCols = listOf(
                dataRel["_system_from"],
                dataRel["_valid_from"],
                dataRel["_valid_to"],
                dataRel["_iid"]
            )

            val contentCols = writeContentMetadata.takeIf { it }
                ?.let { putReader?.keys?.mapNotNull { putReader.keyReader(it) } }
                .orEmpty()

            pageMetaWriter.writeMetadata(temporalCols + contentCols)

            pageIdxWtr.writeInt(pageIdx++)
            leafWtr.endStruct()
            metaRel.endRow()

            dataFileWriter.writePage()
            dataRel.clear()

            return metaPos
        }

        fun writeIidBranch(idxs: IntArray): Int {
            val rowIdx = nodeWtr.valueCount

            for (idx in idxs)
                if (idx < 0) iidBranchElWtr.writeNull() else iidBranchElWtr.writeInt(idx)

            iidBranchWtr.endList()
            metaRel.endRow()

            return rowIdx
        }

        private val metaFilePath = tableName.metaFilePath(trieKey)

        /**
         * @return the size of the data file
         */
        fun end(): FileSize {
            val dataFileSize = dataFileWriter.end()

            bufferPool.openArrowWriter(metaFilePath, metaRel).use { metaFileWriter ->
                metaFileWriter.writePage()
                metaFileWriter.end()
            }

            return dataFileSize
        }

        override fun close() {
            metaRel.close()
            dataFileWriter.close()
            dataRel.close()
        }
    }

    fun writeLiveTrie(tableName: TableName, trieKey: TrieKey, trie: MemoryHashTrie, dataRel: Relation): FileSize =
        OpenWriter(tableName, trieKey, dataRel.schema).use { writer ->
            val copier = writer.dataRel.rowCopier(dataRel)

            fun MemoryHashTrie.Node.writeNode(): Int =
                when (this) {
                    is MemoryHashTrie.Branch ->
                        writer.writeIidBranch(IntArray(iidChildren.size) { idx -> iidChildren[idx]?.writeNode() ?: -1 })

                    is MemoryHashTrie.Leaf -> {
                        data.forEach { idx -> copier.copyRow(idx) }
                        writer.writeLeaf()
                    }
                }

            trie.compactLogs().rootNode.writeNode()

            writer.end()
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

    fun writeRelation(tableName: TableName, trieKey: TrieKey, rel: Relation, pageSize: Int): FileSize =
        OpenWriter(tableName, trieKey, rel.schema).use { writer ->
            val dataRel = writer.dataRel
            val rowCopier = dataRel.rowCopier(rel)
            val iidReader = rel["_iid"]!!
            val startPtr = ArrowBufPointer()
            val endPtr = ArrowBufPointer()

            fun Selection.soloIid(): Boolean =
                iidReader.getPointer(first(), startPtr) == iidReader.getPointer(last(), endPtr)

            fun writeSubtree(depth: Int, sel: Selection): Int =
                when {
                    Thread.interrupted() -> throw InterruptedException()

                    sel.isEmpty() -> writer.writeNull()

                    sel.size <= pageSize || depth >= 64 || sel.soloIid() -> {
                        for (idx in sel) rowCopier.copyRow(idx)

                        val pos = writer.writeLeaf()
                        dataRel.clear()
                        pos
                    }

                    else -> writer.writeIidBranch(
                        sel.iidPartitions(iidReader, depth)
                            .map { if (it != null) writeSubtree(depth + 1, it) else -1 }
                            .toIntArray())
                }

            writeSubtree(0, IntArray(rel.rowCount) { idx -> idx })

            writer.end()
        }
}
