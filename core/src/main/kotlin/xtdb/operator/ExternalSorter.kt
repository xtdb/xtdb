package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.api.error.Fault
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorTypes
import xtdb.util.closeAll
import xtdb.util.closeOnCatch
import xtdb.util.safeMap
import xtdb.util.useAll
import java.nio.channels.Channels
import java.nio.file.Files
import java.nio.file.Path
import java.util.PriorityQueue
import java.util.function.Consumer
import java.util.function.Supplier
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively

private const val K_WAY = 4

/**
 * Sorts pages with the columns in [vecTypes], keeping only the first [rowLimit] rows.
 *
 * [append] copies each page in; call [finish] once, after the last [append].
 * Whenever it holds more than [blockSize] rows, it writes a sorted run to a spill directory, which [openSpillDir] creates on the first spill.
 *
 * Closing the sorter releases its memory and deletes any spill files, whether or not [finish] was reached.
 */
class ExternalSorter(
    private val allocator: BufferAllocator,
    private val vecTypes: VectorTypes,
    private val comparatorFactory: ComparatorFactory,
    private val rowLimit: Long,
    private val blockSize: Int,
    private val openSpillDir: Supplier<Path>,
) : AutoCloseable {

    /**
     * Orders rows of [left] against rows of [right], by row index.
     * Both relations have the columns in [vecTypes]; [left] and [right] may be the same relation.
     */
    fun interface ComparatorFactory {
        fun build(left: RelationReader, right: RelationReader): Comparator<Int>
    }

    private class Spill(val dir: Path) {
        val runs = mutableListOf<Path>()
    }

    private val accRel = Relation(allocator, vecTypes)
    private var spill: Spill? = null
    private var out: ICursor? = null

    private fun topIdxs(rel: RelationReader): IntArray {
        val idxs = (0 until rel.rowCount).sortedWith(comparatorFactory.build(rel, rel)).toIntArray()
        return if (rowLimit < idxs.size) idxs.copyOf(rowLimit.toInt()) else idxs
    }

    private fun sortFile(dir: Path, batchIdx: Int, fileIdx: Int): Path =
        dir.resolve("temp-sort-$batchIdx-$fileIdx.arrow")

    private fun writeRun(spill: Spill) {
        val run = sortFile(spill.dir, 0, spill.runs.size)
        accRel.select(topIdxs(accRel)).openDirectSlice(allocator).use { rel ->
            rel.startUnload(run).use { unl ->
                unl.writePage()
                unl.end()
            }
        }
        spill.runs.add(run)
    }

    fun append(rel: RelationReader) {
        accRel.append(rel)

        if (rowLimit <= blockSize / 2 && blockSize < accRel.rowCount) {
            accRel.select(topIdxs(accRel)).openDirectSlice(allocator).use { topRel ->
                accRel.clear()
                accRel.append(topRel)
            }
        }

        if (blockSize < accRel.rowCount) {
            writeRun(spill ?: Spill(openSpillDir.get()).also { spill = it })
            accRel.clear()
        }
    }

    private fun kWayMerge(dir: Path, files: List<Path>, batchIdx: Int, fileIdx: Int): Path {
        val k = files.size
        val outFile = sortFile(dir, batchIdx, fileIdx)

        files.safeMap { file ->
            try {
                Relation.loader(allocator, file)
            } catch (e: Exception) {
                throw Fault("unable to read file", "xtdb/unreadable-sort-run", mapOf("file-name" to file.toString()), e)
            }
        }.useAll { loaders ->
            loaders.safeMap { Relation(allocator, it.schema) }.useAll { rels ->
                Relation(allocator, vecTypes).use { outRel ->
                    outRel.startUnload(Channels.newChannel(Files.newOutputStream(outFile))).use { outUnl ->
                        val copiers = rels.map { it.rowCopier(outRel) }
                        val positions = IntArray(k)

                        fun loadNextRel(i: Int): Boolean =
                            loaders[i].loadNextPage(rels[i]).also { loaded -> if (loaded) positions[i] = 0 }

                        repeat(k) { loadNextRel(it) }

                        val cmps = Array(k) { arrayOfNulls<Comparator<Int>>(k) }
                        for (i in 0 until k) for (j in 0 until i) cmps[j][i] = comparatorFactory.build(rels[j], rels[i])

                        val pq = PriorityQueue<Int> { i, j ->
                            if (i < j) cmps[i][j]!!.compare(positions[i], positions[j])
                            else -cmps[j][i]!!.compare(positions[j], positions[i])
                        }

                        for (i in 0 until k) if (rels[i].rowCount > 0) pq.add(i)

                        var copied = 0L
                        while (copied < rowLimit && pq.isNotEmpty()) {
                            val i = pq.poll()
                            val pos = positions[i]
                            positions[i] = pos + 1
                            copiers[i].copyRow(pos)

                            if (pos + 1 < rels[i].rowCount || loadNextRel(i)) pq.add(i)

                            if (blockSize < outRel.rowCount) {
                                outUnl.writePage()
                                outRel.clear()
                            }

                            copied++
                        }

                        if (outRel.rowCount > 0) outUnl.writePage()
                        outUnl.end()
                    }
                }
            }
        }

        return outFile
    }

    private fun mergeRuns(dir: Path, runs: List<Path>): Path {
        var runs = runs
        var batchIdx = 1
        while (runs.size > 1) {
            runs = runs.chunked(K_WAY).mapIndexed { fileIdx, files ->
                try {
                    kWayMerge(dir, files, batchIdx, fileIdx)
                } finally {
                    files.forEach(Files::delete)
                }
            }
            batchIdx++
        }
        return runs.first()
    }

    private class InMemoryCursor(private var rel: RelationReader?) : ICursor {
        override val cursorType get() = "in-memory-sort"
        override val childCursors get() = emptyList<ICursor>()

        override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
            val rel = rel ?: return false
            this.rel = null
            c.accept(rel)
            return true
        }
    }

    private class SpilledCursor(private val loader: Relation.Loader, private val rel: Relation) : ICursor {
        override val cursorType get() = "spilled-sort"
        override val childCursors get() = emptyList<ICursor>()

        override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
            if (!loader.loadNextPage(rel)) return false
            c.accept(rel)
            return true
        }

        override fun close() = listOf(rel, loader).closeAll()
    }

    /**
     * Returns the sorted pages.
     * The cursor is valid until the sorter is closed, and is closed with it; each page it hands out is valid only until its next `tryAdvance`.
     */
    fun finish(): ICursor {
        val spill = spill
        val cursor =
            if (spill == null) {
                InMemoryCursor(if (accRel.rowCount > 0) accRel.select(topIdxs(accRel)) else null)
            } else {
                if (accRel.rowCount > 0) writeRun(spill)
                val sortedFile = mergeRuns(spill.dir, spill.runs.toList())
                Relation.loader(allocator, sortedFile).closeOnCatch { loader ->
                    SpilledCursor(loader, Relation(allocator, loader.schema))
                }
            }

        out = cursor
        return cursor
    }

    @OptIn(ExperimentalPathApi::class)
    override fun close() {
        try {
            listOf(out, accRel).closeAll()
        } finally {
            spill?.dir?.deleteRecursively()
        }
    }
}
