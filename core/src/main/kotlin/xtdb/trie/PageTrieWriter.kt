package xtdb.trie

import com.carrotsearch.hppc.ByteArrayList
import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.storage.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.VectorReader
import xtdb.compactor.PageTree
import xtdb.compactor.PageTree.Companion.asTree
import xtdb.log.proto.TrieMetadata
import xtdb.table.TableSlug
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.deleteOnCatch
import xtdb.util.useAll
import java.nio.file.Path
import java.util.TreeMap
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists

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

        internal fun Selection.recencyRange(recencyReader: VectorReader): RecencyRange {
            var min = RecencyRange.EMPTY.min
            var max = RecencyRange.EMPTY.max

            for (idx in this) {
                val recency = recencyReader.getLong(idx)
                if (recency < min) min = recency
                if (recency > max) max = recency
            }

            return RecencyRange(min, max)
        }

        internal class RecencyPart(val idx: Int, val range: RecencyRange, val sel: Selection)

        /**
         * Splits the selection into [k] equal-width parts of [range].
         *
         * @return the parts that hold rows, oldest first, each with its rows in selection order and the range of their recency
         */
        internal fun Selection.recencyParts(recencyReader: VectorReader, range: RecencyRange, k: Int): List<RecencyPart> {
            val idxs = Array(k) { IntArrayList() }
            val mins = LongArray(k) { RecencyRange.EMPTY.min }
            val maxs = LongArray(k) { RecencyRange.EMPTY.max }

            for (idx in this) {
                val recency = recencyReader.getLong(idx)
                val part = range.partIdx(recency, k)
                idxs[part].add(idx)
                if (recency < mins[part]) mins[part] = recency
                if (recency > maxs[part]) maxs[part] = recency
            }

            return (0 until k).filterNot { idxs[it].isEmpty }
                .map { part -> RecencyPart(part, RecencyRange(mins[part], maxs[part]), idxs[part].toArray()) }
        }

        /**
         * Whether a node at IID depth [depth], having branched on recency [levelsUsed] times, branches on recency next:
         * below the IID levels the file's part fixes, IID and recency alternate, IID first.
         */
        internal fun recencyTurn(depth: Int, levelsUsed: Int, pinnedDepth: Int): Boolean {
            val base = maxOf(pinnedDepth, 1)
            return depth >= base && levelsUsed <= depth - base
        }

        /**
         * Splits the selection into pages of roughly equal size by recency, each keyed by the most recent row in it.
         *
         * Two pages may share a key; each page stays in selection order.
         */
        internal fun Selection.recencyPages(recencyReader: VectorReader, pageSize: Int): List<Pair<RecencyMicros, Selection>> {
            val pageCount = (size + pageSize - 1) / pageSize
            val byRecency = sortedBy { recencyReader.getLong(it) }

            return List(pageCount) { page ->
                val pageSel = byRecency.subList(
                    (page.toLong() * size / pageCount).toInt(),
                    ((page + 1).toLong() * size / pageCount).toInt()
                )

                recencyReader.getLong(pageSel.last()) to pageSel.sorted().toIntArray()
            }
        }
    }

    /**
     * Data carrying [Trie.RECENCY_COL] branches on recency as well as IID, by equal-width parts of each node's recency range.
     *
     * @param pinnedDepth the number of IID levels fixed by the file's part
     */
    suspend fun writePageTree(
        table: TableSlug, trieKey: TrieKey,
        loader: Relation.Loader, pageTree: PageTree?,
        pageSize: Int, pinnedDepth: Int
    ): Pair<FileSize, TrieMetadata> =
        DataFileWriter(al, bp, table, trieKey, loader.schema).use { dataFileWriter ->
            val dataRel = dataFileWriter.dataRel

            val startPtr = ArrowBufPointer()
            val endPtr = ArrowBufPointer()

            MetadataFileWriter(al, bp, table, trieKey, dataFileWriter.dataRel, calculateBlooms)
                .use { metaFileWriter ->
                    Relation(al, loader.schema).use { inRel ->
                        val iidReader = inRel["_iid"]
                        val recencyReader = inRel.vectorForOrNull(Trie.RECENCY_COL)

                        fun Selection.soloIid(): Boolean =
                            iidReader.getPointer(first(), startPtr) == iidReader.getPointer(last(), endPtr)

                        val rowCopier = inRel.rowCopier(dataRel)

                        fun writeLeaf(): RowIndex = metaFileWriter.writeLeaf().also { dataFileWriter.writePage() }

                        fun writeLeaf(sel: Selection): RowIndex {
                            rowCopier.copyRows(sel)
                            return writeLeaf()
                        }

                        fun writeSubtree(depth: Int, levelsUsed: Int, sel: Selection): RowIndex {
                            if (Thread.interrupted()) throw InterruptedException()

                            if (sel.isEmpty()) return metaFileWriter.writeNull()
                            if (sel.size <= pageSize) return writeLeaf(sel)

                            val solo = depth >= 64 || sel.soloIid()

                            if (recencyReader == null) {
                                return if (solo) writeLeaf(sel)
                                else metaFileWriter.writeIidBranch(
                                    sel.iidPartitions(iidReader, depth)
                                        .map { if (it != null) writeSubtree(depth + 1, levelsUsed, it) else -1 }
                                        .toIntArray()
                                )
                            }

                            // one entity's versions can't branch on IID, so they keep branching on recency
                            val range = sel.takeIf { solo || recencyTurn(depth, levelsUsed, pinnedDepth) }?.recencyRange(recencyReader)

                            return when {
                                range != null && range.splittable -> metaFileWriter.writeRecencyBranch(
                                    sel.recencyParts(recencyReader, range, RecencyRange.splitCount(sel.size, pageSize))
                                        .map { it.range.max to writeSubtree(depth, levelsUsed + 1, it.sel) }
                                )

                                solo -> metaFileWriter.writeRecencyBranch(
                                    sel.recencyPages(recencyReader, pageSize).map { (key, pageSel) -> key to writeLeaf(pageSel) }
                                )

                                else -> metaFileWriter.writeIidBranch(
                                    sel.iidPartitions(iidReader, depth)
                                        .map { if (it != null) writeSubtree(depth + 1, levelsUsed, it) else -1 }
                                        .toIntArray()
                                )
                            }
                        }

                        class Bucket(
                            val path: Path,
                            private val rel: Relation,
                            private val unloader: Relation.RelationUnloader,
                        ) : AutoCloseable {
                            private val copier = inRel.rowCopier(rel)
                            val leaves = mutableListOf<PageTree.Leaf>()
                            var recency = RecencyRange.EMPTY
                                private set

                            fun writePage(sel: Selection, part: ByteArrayList, pageRecency: RecencyRange) {
                                copier.copyRows(sel)
                                unloader.writePage()
                                leaves += PageTree.Leaf(leaves.size, part, sel.size, pageRecency)
                                recency += pageRecency
                                rel.clear()
                            }

                            fun end() = unloader.end()

                            override fun close() = listOf(unloader, rel, AutoCloseable { path.deleteIfExists() }).closeAll()
                        }

                        fun openBucket() =
                            createTempFile("recency-bucket", ".arrow").deleteOnCatch { path ->
                                Relation(al, loader.schema).closeOnCatch { rel ->
                                    rel.startUnload(path).closeOnCatch { unloader -> Bucket(path, rel, unloader) }
                                }
                            }

                        // `base` is the depth the source's leaf parts are relative to
                        fun PageTree?.writeSubtree(src: Relation.Loader, base: Int, depth: Int, levelsUsed: Int): RowIndex {
                            if (Thread.interrupted()) throw InterruptedException()

                            return when (this) {
                                null -> metaFileWriter.writeNull()

                                is PageTree.Leaf -> {
                                    if (rowCount > pageSize) {
                                        // split large leaves
                                        src.loadPage(pageIdx, inRel)
                                        writeSubtree(depth, levelsUsed, IntArray(inRel.rowCount) { idx -> idx })
                                    } else {
                                        src.loadPage(pageIdx, dataRel)
                                        writeLeaf()
                                    }
                                }

                                is PageTree.Node -> {
                                    when {
                                        rowCount <= pageSize -> {
                                            for (leaf in leaves) {
                                                src.loadPage(leaf.pageIdx, inRel)
                                                dataRel.append(inRel)
                                            }

                                            writeLeaf()
                                        }

                                        recencyReader != null && recency.splittable && recencyTurn(depth, levelsUsed, pinnedDepth) -> {
                                            val k = RecencyRange.splitCount(rowCount, pageSize)

                                            // spilled to a file per part, so that only a page of each is ever in memory
                                            TreeMap<Int, Bucket>().useAll { buckets ->
                                                for (leaf in leaves) {
                                                    src.loadPage(leaf.pageIdx, inRel)

                                                    val part = ByteArrayList.from(*leaf.part.toArray().copyOfRange(depth - base, leaf.part.size()))

                                                    for (pagePart in IntArray(inRel.rowCount) { it }.recencyParts(recencyReader, recency, k)) {
                                                        buckets.getOrPut(pagePart.idx) { openBucket() }.writePage(pagePart.sel, part, pagePart.range)
                                                    }
                                                }

                                                buckets.values.forEach { it.end() }

                                                metaFileWriter.writeRecencyBranch(
                                                    buckets.values.map { bucket ->
                                                        Relation.loader(al, bucket.path).use { bucketSrc ->
                                                            bucket.recency.max to bucket.leaves.asTree.writeSubtree(bucketSrc, depth, depth, levelsUsed + 1)
                                                        }
                                                    }
                                                )
                                            }
                                        }

                                        else -> metaFileWriter.writeIidBranch(
                                            IntArray(children.size) {
                                                children[it]?.writeSubtree(src, base, depth + 1, levelsUsed) ?: -1
                                            }
                                        )
                                    }
                                }
                            }
                        }

                        pageTree.writeSubtree(loader, 0, 0, 0)

                        Pair(dataFileWriter.end(), metaFileWriter.end())
                    }
                }
        }
}
