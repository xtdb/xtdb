package xtdb.metadata

import com.carrotsearch.hppc.ObjectIntHashMap
import com.carrotsearch.hppc.ObjectIntMap
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.memory.BufferAllocator
import xtdb.BufferPool
import xtdb.arrow.Relation
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

private typealias PageIdxsMap = ObjectIntMap<PageMetadata.PageIndexKey>

class PageMetadata private constructor(private val rel: Relation, private val pageIdxs: PageIdxsMap) : AutoCloseable {

    val trie = ArrowHashTrie(rel["nodes"])

    companion object {
        @JvmStatic
        fun factory(al: BufferAllocator, bp: BufferPool, cacheSize: Long = 128) = Factory(al, bp, cacheSize)
    }

    val metadataLeafReader: VectorReader = rel["nodes"]["leaf"]

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

    internal data class PageIndexKey(val columnName: String, val pageIdx: Int)

    fun rowIndex(columnName: String, pageIdx: Int) =
        pageIdxs.getOrDefault(PageIndexKey(columnName, pageIdx), -1)

    fun temporalMetadata(pageIdx: Int): TemporalMetadata {
        // it seems in some tests we have files without any temporal values in...?
        val minReader = requireNotNull(minReader)
        val maxReader = requireNotNull(maxReader)

        val systemFromIdx = pageIdxs[PageIndexKey("_system_from", pageIdx)]
        val validFromIdx = pageIdxs[PageIndexKey("_valid_from", pageIdx)]
        val validToIdx = pageIdxs[PageIndexKey("_valid_to", pageIdx)]

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

    class Factory(al: BufferAllocator, private val bp: BufferPool, cacheSize: Long) : AutoCloseable {
        companion object {
            private fun readPageIdxs(metadataReader: VectorReader): PageIdxsMap {
                val pageIdxs: PageIdxsMap = ObjectIntHashMap()
                val dataPageIdxReader = metadataReader["data-page-idx"]

                val columnsReader = metadataReader["columns"]
                val columnReader = columnsReader.listElements
                val colNameReader = columnReader["col-name"]
                val rootColReader = columnReader["root-col?"]

                val colNames = HashSet<ColumnName>()

                for (idx in 0 until metadataReader.valueCount) {
                    if (metadataReader.isNull(idx) || columnsReader.isNull(idx)) continue

                    val colsStartIdx = columnsReader.getListStartIndex(idx)
                    val dataPageIdx = dataPageIdxReader.takeUnless { it.isNull(idx) }?.getInt(idx) ?: -1

                    for (colsDataIdx in colsStartIdx until colsStartIdx + columnsReader.getListCount(idx)) {
                        val colName = colNameReader.getObject(colsDataIdx) as String
                        colNames.add(colName)

                        if (rootColReader.getBoolean(colsDataIdx)) {
                            pageIdxs.put(PageIndexKey(colName, dataPageIdx), colsDataIdx)
                        }
                    }
                }

                return pageIdxs
            }
        }

        private val pageIdxCache: Cache<Path, PageIdxsMap> =
            Caffeine.newBuilder().maximumSize(cacheSize).build()

        private val al = al.openChildAllocator("metadata-mgr")

        fun openPageMetadata(metaFilePath: Path): PageMetadata =
            bp.getRecordBatch(metaFilePath, 0).use { rb ->
                val footer = bp.getFooter(metaFilePath)
                Relation.fromRecordBatch(al, footer.schema, rb).closeOnCatch { rel ->
                    val metadataReader = rel["nodes"]["leaf"]

                    val pageIdxs = pageIdxCache.get(metaFilePath) { readPageIdxs(metadataReader) }
                    PageMetadata(rel, pageIdxs)
                }
            }

        override fun close() {
            al.close()
        }
    }
}