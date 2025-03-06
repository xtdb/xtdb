package xtdb.trie

import com.google.protobuf.ByteString
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.types.pojo.Schema
import org.roaringbitmap.RoaringBitmap
import xtdb.ArrowWriter
import xtdb.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.Vector
import xtdb.arrow.VectorReader
import xtdb.bloom.bloomHashes
import xtdb.bloom.toByteBuffer
import xtdb.compactor.PageTree
import xtdb.log.proto.TrieMetadata
import xtdb.metadata.PageMetadataWriter
import xtdb.trie.HashTrie.Companion.LEVEL_WIDTH
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.Trie.metaRelSchema
import xtdb.util.requiringResolve
import xtdb.util.toByteArray

private typealias Selection = IntArray

class TrieWriter(
    private val allocator: BufferAllocator,
    private val bufferPool: BufferPool,
    private val writeContentMetadata: Boolean
) {

    private inner class OpenWriter(
        private val tableName: TableName, private val trieKey: TrieKey, dataSchema: Schema, private val writeTrieMetadata: Boolean = true
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

        private val trieMetadataBuilder = TrieMetadata.newBuilder()
        private val iidBloom = RoaringBitmap()

        fun writeNull(): RowIndex {
            val pos = nodeWtr.valueCount
            nullBranchWtr.writeNull()
            return pos
        }

        private fun getMinMax(col: Vector, initMin: Long, initMax: Long): Pair<Long, Long> {
            var min = initMin
            var max = initMax
            for (i in 0 until col.valueCount) {
                val v = col.getLong(i)
                min = minOf(min, v)
                max = maxOf(max, v)
            }
            return min to max
        }

        fun writeLeaf(): RowIndex {
            val putReader = dataRel["op"]!!.legReader("put")
            val metaPos = nodeWtr.valueCount

            val systemFrom = dataRel["_system_from"]!!
            val validFrom = dataRel["_valid_from"]!!
            val validTo = dataRel["_valid_to"]!!
            val iidVec = dataRel["_iid"]!!

            val temporalCols = listOf(systemFrom, validFrom, validTo, iidVec)

            if(writeTrieMetadata) {
                val (minValidFrom, maxValidFrom) = getMinMax(validFrom ,trieMetadataBuilder.minValidFrom, trieMetadataBuilder.maxValidFrom)
                trieMetadataBuilder.minValidFrom = minValidFrom
                trieMetadataBuilder.maxValidFrom = maxValidFrom

                val (minValidTo, maxValidTo) = getMinMax(validTo,trieMetadataBuilder.minValidTo, trieMetadataBuilder.maxValidTo)
                trieMetadataBuilder.minValidTo = minValidTo
                trieMetadataBuilder.maxValidTo = maxValidTo

                val (minSystemFrom, maxSystemFrom) = getMinMax(systemFrom, trieMetadataBuilder.minSystemFrom, trieMetadataBuilder.maxSystemFrom)
                trieMetadataBuilder.minSystemFrom = minSystemFrom
                trieMetadataBuilder.maxSystemFrom = maxSystemFrom

                trieMetadataBuilder.rowCount = iidVec.valueCount.toLong()

                for(i in 0 until iidVec.valueCount) {
                    iidBloom.add(*bloomHashes(iidVec , i))
                }
            }

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
        fun end(): Pair<FileSize, TrieMetadata> {
            val dataFileSize = dataFileWriter.end()

            bufferPool.openArrowWriter(metaFilePath, metaRel).use { metaFileWriter ->
                metaFileWriter.writePage()
                metaFileWriter.end()
            }

            if(writeTrieMetadata) {
                trieMetadataBuilder.iidBloom  = ByteString.copyFrom(iidBloom.toByteBuffer().toByteArray())
            }

            return Pair(dataFileSize, trieMetadataBuilder.build())
        }

        override fun close() {
            metaRel.close()
            dataFileWriter.close()
            dataRel.close()
        }
    }

    fun writeLiveTrie(tableName: TableName, trieKey: TrieKey, trie: MemoryHashTrie, dataRel: Relation): FileSize =
        OpenWriter(tableName, trieKey, dataRel.schema, false).use { writer ->
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

            writer.end().first
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

    private fun OpenWriter.writeRelation(rel: Relation, depth: Int, pageSize: Int): RowIndex {
        val rowCopier = dataRel.rowCopier(rel)
        val iidReader = rel["_iid"]!!
        val startPtr = ArrowBufPointer()
        val endPtr = ArrowBufPointer()

        fun Selection.soloIid(): Boolean =
            iidReader.getPointer(first(), startPtr) == iidReader.getPointer(last(), endPtr)

        fun writeSubtree(depth: Int, sel: Selection): RowIndex =
            when {
                Thread.interrupted() -> throw InterruptedException()

                sel.isEmpty() -> writeNull()

                sel.size <= pageSize || depth >= 64 || sel.soloIid() -> {
                    for (idx in sel) rowCopier.copyRow(idx)

                    writeLeaf()
                }

                else -> writeIidBranch(
                    sel.iidPartitions(iidReader, depth)
                        .map { if (it != null) writeSubtree(depth + 1, it) else -1 }
                        .toIntArray())
            }

        return writeSubtree(depth, IntArray(rel.rowCount) { idx -> idx })
    }

    fun writePageTree(
        tableName: TableName, trieKey: TrieKey,
        loader: Relation.Loader, pageTree: PageTree?,
        pageSize: Int
    ): Pair<FileSize, TrieMetadata> =
        OpenWriter(tableName, trieKey, loader.schema).use { writer ->
            Relation(allocator, loader.schema).use { inRel ->
                fun PageTree?.writeSubtree(depth: Int): RowIndex {
                    if (Thread.interrupted()) throw InterruptedException()

                    return when (this) {
                        null -> writer.writeNull()

                        is PageTree.Leaf -> {
                            if (rowCount > pageSize) {
                                // split large leaves
                                loader.loadPage(pageIdx, inRel)
                                writer.writeRelation(inRel, depth, pageSize)
                            } else {
                                loader.loadPage(pageIdx, writer.dataRel)
                                writer.writeLeaf()
                            }
                        }

                        is PageTree.Node -> {
                            if (rowCount > pageSize) {
                                val idxs = IntArray(children.size) { children[it]?.writeSubtree(depth + 1) ?: -1 }
                                writer.writeIidBranch(idxs)
                            } else {
                                // combine small leaves
                                inRel.clear()
                                for (leaf in leaves) {
                                    loader.loadPage(leaf.pageIdx, inRel)
                                    writer.dataRel.append(inRel)
                                }
                                writer.writeLeaf()
                            }
                        }
                    }
                }

                pageTree.writeSubtree(0)

                writer.end()
            }
        }
}
