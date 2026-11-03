package xtdb.metadata

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.storage.BufferPool
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.log.proto.TemporalMetadata
import xtdb.time.MICRO_HZ
import xtdb.trie.ArrowHashTrie
import xtdb.trie.ColumnName
import xtdb.util.closeOnCatch
import xtdb.util.openChildAllocator
import java.nio.file.Path
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

internal val UNBOUND_TEMPORAL_METADATA =
    TemporalMetadata.newBuilder()
        .setMinValidFrom(MIN_LONG)
        .setMaxValidFrom(MAX_LONG)
        .setMinValidTo(MIN_LONG)
        .setMaxValidTo(MAX_LONG)
        .setMinSystemFrom(MIN_LONG)
        .setMaxSystemFrom(MAX_LONG)
        .build()

class PageMetadata private constructor(private val rel: Relation, private val pageIdxs: PageIdxs) : AutoCloseable {

    val trie = ArrowHashTrie(rel["nodes"])

    internal class PageIdxs(private val colsStartIdxs: IntArray, private val colOffsets: Map<ColumnName, Int>) {
        operator fun get(columnName: ColumnName, pageIdx: Int): Int {
            val colOffset = colOffsets[columnName] ?: return -1
            if (pageIdx !in colsStartIdxs.indices) return -1
            val colsStartIdx = colsStartIdxs[pageIdx]
            return if (colsStartIdx < 0) -1 else colsStartIdx + colOffset
        }
    }

    companion object {
        @JvmStatic
        fun factory(al: BufferAllocator, bp: BufferPool) = Factory(al, bp)

        @JvmStatic
        fun open(al: BufferAllocator, metaFilePath: Path): PageMetadata =
            Relation.loader(al, metaFilePath).use { loader ->
                loader.loadPage(0, al).closeOnCatch { rel ->
                    PageMetadata(rel, readPageIdxs(rel))
                }
            }

        private fun readPageIdxs(rel: RelationReader): PageIdxs {
            val metadataReader = rel["nodes"]["leaf"]
            val dataPageIdxReader = metadataReader["data-page-idx"]
            val columnsReader = metadataReader["columns"]

            val leafIdxs = (0 until metadataReader.valueCount)
                .filterNot { metadataReader.isNull(it) || columnsReader.isNull(it) }

            val colsStartIdxs = IntArray(metadataReader.valueCount) { -1 }
            for (leafIdx in leafIdxs)
                colsStartIdxs[dataPageIdxReader.getInt(leafIdx)] = columnsReader.getListStartIndex(leafIdx)

            // every leaf of a metadata file lists its column entries in the same order (asserted in MetadataFileWriter),
            // so a column's position within the first leaf's entries is its position within every leaf's
            val colOffsets = HashMap<ColumnName, Int>()
            leafIdxs.firstOrNull()?.let { leafIdx ->
                val columnReader = columnsReader.listElements
                val colNameReader = columnReader["col-name"]
                val rootColReader = columnReader["root-col?"]
                val colsStartIdx = columnsReader.getListStartIndex(leafIdx)

                for (colOffset in 0 until columnsReader.getListCount(leafIdx))
                    if (rootColReader.getBoolean(colsStartIdx + colOffset))
                        colOffsets[colNameReader.getObject(colsStartIdx + colOffset) as String] = colOffset
            }

            return PageIdxs(colsStartIdxs, colOffsets)
        }
    }

    val metadataLeafReader: VectorReader = rel["nodes"]["leaf"]

    val pageCount get() = metadataLeafReader.valueCount

    private val minReader: VectorReader?
    private val maxReader: VectorReader?

    init {
        val temporalColTypesReader =
            metadataLeafReader
                .vectorForOrNull("columns")
                ?.listElements
                ?.vectorForOrNull("date-times")

        minReader = temporalColTypesReader?.vectorForOrNull("min")
        maxReader = temporalColTypesReader?.vectorForOrNull("max")
    }

    fun rowIndex(columnName: String, pageIdx: Int) = pageIdxs[columnName, pageIdx]

    fun temporalMetadata(pageIdx: Int): TemporalMetadata {
        // it seems in some tests we have files without any temporal values in...?
        val minReader = requireNotNull(minReader)
        val maxReader = requireNotNull(maxReader)

        val systemFromIdx = pageIdxs["_system_from", pageIdx]
        val validFromIdx = pageIdxs["_valid_from", pageIdx]
        val validToIdx = pageIdxs["_valid_to", pageIdx]

        // a 0-row page (e.g. an empty table) writes its temporal columns' metadata but no min/max,
        // so the bounds are absent. Report the empty-set (proto defaults, min > max): the page then
        // matches no temporal predicate and is skipped, rather than dereferencing the missing value.
        if (minReader.isNull(validFromIdx)) return TemporalMetadata.newBuilder().build()

        return TemporalMetadata.newBuilder()
            .setMinValidFrom(floor(minReader.getDouble(validFromIdx) * MICRO_HZ).toLong())
            .setMaxValidFrom(ceil(maxReader.getDouble(validFromIdx) * MICRO_HZ).toLong())
            .setMinValidTo(floor(minReader.getDouble(validToIdx) * MICRO_HZ).toLong())
            .setMaxValidTo(ceil(maxReader.getDouble(validToIdx) * MICRO_HZ).toLong())
            .setMinSystemFrom(floor(minReader.getDouble(systemFromIdx) * MICRO_HZ).toLong())
            .setMaxSystemFrom(ceil(maxReader.getDouble(systemFromIdx) * MICRO_HZ).toLong())
            .build()
    }

    override fun close() = rel.close()

    // TODO inline this into BP
    class Factory(al: BufferAllocator, private val bp: BufferPool) : AutoCloseable {
        private val al = al.openChildAllocator("metadata-mgr")

        fun openPageMetadataSync(metaFilePath: Path): PageMetadata = runBlocking { openPageMetadata(metaFilePath) }

        suspend fun openPageMetadata(metaFilePath: Path): PageMetadata =
            bp.getRecordBatch(metaFilePath, 0).use { rb ->
                val footer = bp.getFooter(metaFilePath)
                Relation.fromRecordBatch(al, footer.schema, rb).closeOnCatch { rel ->
                    PageMetadata(rel, readPageIdxs(rel))
                }
            }

        override fun close() {
            al.close()
        }
    }
}