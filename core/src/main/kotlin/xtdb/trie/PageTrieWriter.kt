package xtdb.trie

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.storage.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.VectorReader
import xtdb.compactor.PageTree
import xtdb.compactor.PageTree.Companion.asTree
import xtdb.log.proto.TrieMetadata
import xtdb.table.TableSlug
import xtdb.util.useAll
import java.util.TreeMap

private typealias Selection = IntArray

class PageTrieWriter(
    private val al: BufferAllocator, private val bp: BufferPool,
    private val calculateBlooms: Boolean
) {

    companion object {
        internal fun Selection.partitionSlices(partIdxs: IntArray) =
            Array(DEFAULT_LEVEL_WIDTH) { partition ->
                val cur = partIdxs[partition]
                val nxt = if (partition == partIdxs.lastIndex) size else partIdxs[partition + 1]

                if (cur == nxt) null else sliceArray(cur..<nxt)
            }

        internal fun Selection.iidPartitions(iidReader: VectorReader, level: Int): Array<Selection?> {
            val iidPtr = ArrowBufPointer()

            // for each partition, find the starting index in the selection
            val partIdxs = IntArray(DEFAULT_LEVEL_WIDTH) { partition ->
                var left = 0
                var right = size
                var mid: Int
                while (left < right) {
                    mid = (left + right) / 2

                    val bucket = Bucketer.DEFAULT.bucketFor(iidReader.getPointer(this[mid], iidPtr), level)

                    if (bucket < partition) left = mid + 1 else right = mid
                }

                left
            }

            // slice the selection array for each partition
            return partitionSlices(partIdxs)
        }
    }

    private fun writeRecencyCells(
        loader: Relation.Loader, inputPages: List<PageTree.Leaf>, part: ByteArray,
        inRel: Relation, dataFileWriter: DataFileWriter, metaFileWriter: MetadataFileWriter, pageSize: Int
    ): RowIndex {
        if (inputPages.isEmpty()) return metaFileWriter.writeNull()

        val recency = inputPages.fold(RecencyRange.EMPTY) { acc, page -> acc + page.recency }
        val width = RecencyCells.width(inputPages.sumOf { it.rowCount.toLong() }, recency, pageSize)

        val iids = inRel["_iid"]
        val recencies = inRel[Trie.RECENCY_COL]

        fun forEachRow(f: (Int) -> Unit) {
            for (page in inputPages) {
                if (Thread.interrupted()) throw InterruptedException()
                loader.loadPage(page.pageIdx, inRel)
                repeat(inRel.rowCount, f)
            }
        }

        return RowBuffers(al, loader.schema, inRel, dataFileWriter.dataRel).use { buffers ->
            fun openTrie(startDepth: Int) = StreamingIidTrie(buffers, metaFileWriter, dataFileWriter, startDepth, pageSize)

            if (width == null) {
                openTrie(0).use { trie ->
                    forEachRow { trie.add(iids, it) }
                    trie.end()
                }
            } else {
                val cellsBranch = TreeMap<RecencyMicros, StreamingIidTrie>().useAll { cells ->
                    forEachRow { idx ->
                        cells.getOrPut(RecencyCells.boundary(recencies.getLong(idx), width)) { openTrie(part.size) }
                            .add(iids, idx)
                    }

                    metaFileWriter.writeRecencyBranch(cells.map { (boundary, trie) -> boundary to trie.end() })
                }

                part.indices.reversed().fold(cellsBranch) { child, depth ->
                    metaFileWriter.writeIidBranch(IntArray(DEFAULT_LEVEL_WIDTH) { if (it == part[depth].toInt()) child else -1 })
                }
            }
        }
    }

    /**
     * Data carrying [Trie.RECENCY_COL] branches once on recency, below the IID levels [part] pins, into [RecencyCells];
     * each cell, or the whole file where it has none, is an IID trie that splits any node of more than [pageSize] rows.
     *
     * @param inputPages the input's pages, in IID order
     * @param part the IID levels every row in the input shares; null where it pins none
     */
    suspend fun writePageTree(
        table: TableSlug, trieKey: TrieKey,
        loader: Relation.Loader, inputPages: List<PageTree.Leaf>,
        pageSize: Int, part: ByteArray?
    ): Pair<FileSize, TrieMetadata> =
        DataFileWriter(al, bp, table, trieKey, loader.schema).use { dataFileWriter ->
            val dataRel = dataFileWriter.dataRel

            val startPtr = ArrowBufPointer()
            val endPtr = ArrowBufPointer()

            MetadataFileWriter(al, bp, table, trieKey, dataFileWriter.dataRel, calculateBlooms)
                .use { metaFileWriter ->
                    Relation(al, loader.schema).use { inRel ->
                        val iidReader = inRel["_iid"]

                        fun Selection.soloIid(): Boolean =
                            iidReader.getPointer(first(), startPtr) == iidReader.getPointer(last(), endPtr)

                        val rowCopier = inRel.rowCopierWithReeIid(dataRel)

                        fun writeLeaf(): RowIndex = metaFileWriter.writeLeaf().also { dataFileWriter.writePage() }

                        fun writeSubtree(depth: Int, sel: Selection): RowIndex =
                            when {
                                Thread.interrupted() -> throw InterruptedException()

                                sel.isEmpty() -> metaFileWriter.writeNull()

                                sel.size <= pageSize || depth >= 64 || sel.soloIid() -> {
                                    rowCopier.copyRows(sel)
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

                        if (inRel.vectorForOrNull(Trie.RECENCY_COL) == null)
                            inputPages.asTree.writeSubtree(0)
                        else
                            writeRecencyCells(
                                loader, inputPages, part ?: byteArrayOf(),
                                inRel, dataFileWriter, metaFileWriter, pageSize
                            )

                        Pair(dataFileWriter.end(), metaFileWriter.end())
                    }
                }
        }
}
