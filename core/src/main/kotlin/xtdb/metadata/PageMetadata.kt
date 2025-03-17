package xtdb.metadata

import com.carrotsearch.hppc.ObjectIntHashMap
import com.carrotsearch.hppc.ObjectIntMap
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.arrow.memory.BufferAllocator
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import xtdb.BufferPool
import xtdb.TEMPORAL_COL_TYPE
import xtdb.arrow.Relation
import xtdb.log.proto.TemporalMetadata
import xtdb.toLeg
import xtdb.trie.ArrowHashTrie
import xtdb.trie.ColumnName
import xtdb.util.TemporalBounds
import xtdb.util.TemporalDimension
import xtdb.util.closeAllOnCatch
import xtdb.util.openChildAllocator
import xtdb.vector.IVectorReader
import java.nio.file.Path
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG

private typealias PageIdxsMap = ObjectIntMap<PageMetadata.PageIndexKey>

class PageMetadata private constructor(
    private val rel: Relation,

    /**
     * the set of column names in this metadata file for this table (i.e. not necessarily all of them).
     */
    val columnNames: Set<ColumnName>,

    private val pageIdxs: PageIdxsMap
) : AutoCloseable {

    val trie = ArrowHashTrie(rel["nodes"]!!)

    companion object {
        @JvmStatic
        fun factory(al: BufferAllocator, bp: BufferPool, cacheSize: Long = 128) = Factory(al, bp, cacheSize)

        private val TEMPORAL_COL_TYPE_LEG_NAME: String = TEMPORAL_COL_TYPE.toLeg()
    }

    val metadataLeafReader: IVectorReader =
        rel.oldRelReader.readerForName("nodes").legReader("leaf")

    private val minReader: IVectorReader?
    private val maxReader: IVectorReader?

    init {
        val temporalColTypesReader =
            metadataLeafReader
                .structKeyReader("columns")
                .listElementReader()
                .structKeyReader("types")
                .structKeyReader(TEMPORAL_COL_TYPE_LEG_NAME)

        minReader = temporalColTypesReader?.structKeyReader("min")
        maxReader = temporalColTypesReader?.structKeyReader("max")
    }

    internal data class PageIndexKey(val columnName: String, val pageIdx: Int)

    fun rowIndex(columnName: String, pageIdx: Int) =
        pageIdxs.getOrDefault(PageIndexKey(columnName, pageIdx), -1)

    fun iidBloomBitmap(pageIdx: Int): ImmutableRoaringBitmap? {
        val bloomReader = metadataLeafReader
            .structKeyReader("columns")
            .listElementReader()
            .structKeyReader("bloom")

        val bloomVecIdx = pageIdxs[PageIndexKey("bloom", pageIdx)]
        if (bloomReader.isNull(bloomVecIdx)) return null

        TODO("type error?!")
//        return readBloom(bloomReader, bloomVecIdx)
    }

    fun temporalMetadata(pageIdx: Int): TemporalMetadata {
        // it seems in some tests we have files without any temporal values in...?
        val minReader = requireNotNull(minReader)
        val maxReader = requireNotNull(maxReader)

        val systemFromIdx = pageIdxs[PageIndexKey("_system_from", pageIdx)]
        val validFromIdx = pageIdxs[PageIndexKey("_valid_from", pageIdx)]
        val validToIdx = pageIdxs[PageIndexKey("_valid_to", pageIdx)]

        return TemporalMetadata.newBuilder()
            .setMinValidFrom(minReader.getLong(validFromIdx))
            .setMaxValidFrom(maxReader.getLong(validFromIdx))
            .setMinValidTo(minReader.getLong(validToIdx))
            .setMaxValidTo(maxReader.getLong(validToIdx))
            .setMinSystemFrom(minReader.getLong(systemFromIdx))
            .setMaxSystemFrom(maxReader.getLong(systemFromIdx))
            .build()
    }

    override fun close() {
        rel.close()
    }

    class Factory(al: BufferAllocator, private val bp: BufferPool, cacheSize: Long) : AutoCloseable {
        internal data class PageIdxCacheEntry(val colNames: Set<ColumnName>, val pageIdxs: PageIdxsMap)

        companion object {
            private fun readPageIdxs(metadataReader: IVectorReader): PageIdxCacheEntry {
                val pageIdxs: PageIdxsMap = ObjectIntHashMap()
                val dataPageIdxReader = metadataReader.structKeyReader("data-page-idx")

                val columnsReader = metadataReader.structKeyReader("columns")
                val columnReader = columnsReader.listElementReader()
                val colNameReader = columnReader.structKeyReader("col-name")
                val rootColReader = columnReader.structKeyReader("root-col?")

                val colNames = HashSet<ColumnName>()

                for (idx in 0 until metadataReader.valueCount()) {
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

                return PageIdxCacheEntry(colNames, pageIdxs)
            }
        }

        private val pageIdxCache: Cache<Path, PageIdxCacheEntry> =
            Caffeine.newBuilder().maximumSize(cacheSize).build()

        private val al = al.openChildAllocator("metadata-mgr")

        fun openPageMetadata(metaFilePath: Path): PageMetadata =
            bp.getRecordBatch(metaFilePath, 0).use { rb ->
                val footer = bp.getFooter(metaFilePath)
                Relation.fromRecordBatch(al, footer.schema, rb).closeAllOnCatch { rel ->
                    val rdr = rel.oldRelReader
                    val metadataReader = rdr.readerForName("nodes").legReader("leaf")

                    val (colNames, pageIdxs) = pageIdxCache.get(metaFilePath) { readPageIdxs(metadataReader) }
                    PageMetadata(rel, colNames, pageIdxs)
                }
            }

        override fun close() {
            al.close()
        }
    }
}