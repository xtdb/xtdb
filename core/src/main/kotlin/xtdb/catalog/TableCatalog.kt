package xtdb.catalog

import com.google.protobuf.ByteString
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.asType
import xtdb.arrow.VectorType.Companion.field
import xtdb.arrow.VectorType.Null
import xtdb.block.proto.Partition
import xtdb.block.proto.TableBlock
import xtdb.indexer.LiveTable
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.util.HLL
import xtdb.util.combine
import xtdb.util.toHLL
import java.nio.ByteBuffer

class TableCatalog(
    private val blockCatalog: BlockCatalog,
    private val bufferPool: BufferPool
) {

    internal data class TableMeta(
        val vecTypes: Map<ColumnName, VectorType>,
        val rowCount: Long,
        val hlls: Map<ColumnName, HLL>
    )

    private data class State(
        val blockIdx: Long?,
        val tables: Map<TableRef, TableMeta>
    )

    @Volatile
    private var state = State(null, emptyMap())

    fun rowCount(table: TableRef): Long? = state.tables[table]?.rowCount

    fun getType(table: TableRef, columnName: ColumnName): VectorType? =
        state.tables[table]?.vecTypes?.get(columnName)

    fun getTypes(table: TableRef): Map<ColumnName, VectorType>? =
        state.tables[table]?.vecTypes

    val types: Map<TableRef, Map<ColumnName, VectorType>>
        get() = state.tables.mapValues { (_, meta) -> meta.vecTypes }

    fun refresh(blockIndex: Long) {
        if (state.blockIdx?.let { blockIndex <= it } ?: false) return

        state = State(
            blockIdx = blockIndex,
            tables = loadTablesFromStorage(bufferPool, blockCatalog)
        )
    }

    fun finishBlock(
        tableMetadata: Map<TableRef, LiveTable.FinishedBlock>,
        tablePartitions: Map<TableRef, List<Partition>>
    ): Map<TableRef, TableBlock> {
        val oldTables = state.tables

        val deltaByTable = tableMetadata.mapValues { (_, fb) ->
            TableMeta(fb.vecTypes, fb.rowCount.toLong(), fb.hllDeltas)
        }

        val allTableRefs = oldTables.keys + deltaByTable.keys
        val newTables = allTableRefs.associateWith { mergeTables(oldTables[it], deltaByTable[it]) }

        state = State(
            blockIdx = blockCatalog.currentBlockIndex,
            tables = newTables
        )

        return newTables.mapValues { (table, meta) ->
            buildTableBlock(meta.vecTypes, meta.rowCount, tablePartitions[table].orEmpty(), meta.hlls)
        }
    }

    companion object {
        private fun parseTableBlock(tableBlock: TableBlock) =
            TableMeta(
                tableBlock.arrowSchema.toByteArray()
                    .let { Schema.deserializeMessage(ByteBuffer.wrap(it)) }
                    .fields.associate { field -> field.name to field.asType },
                tableBlock.rowCount,
                tableBlock.columnNameToHllMap.mapValues { (_, bs) -> toHLL(bs.toByteArray()) }
            )

        internal fun loadTablesFromStorage(
            bufferPool: BufferPool,
            blockCatalog: BlockCatalog
        ): Map<TableRef, TableMeta> {
            val blockIndex = blockCatalog.currentBlockIndex ?: return emptyMap()
            return blockCatalog.allTables.associateWith { table ->
                val path = BlockCatalog.tableBlockPath(table, blockIndex)
                parseTableBlock(TableBlock.parseFrom(bufferPool.getByteArray(path)))
            }
        }

        internal fun mergeVecTypes(
            old: Map<ColumnName, VectorType>?, new: Map<ColumnName, VectorType>?
        ): Map<ColumnName, VectorType> =
            when {
                old == null -> new.orEmpty()
                new == null -> old
                else -> {
                    val allCols = old.keys + new.keys
                    allCols.associateWith { col -> mergeTypes(old[col] ?: Null, new[col] ?: Null) }
                }
            }

        internal fun mergeHlls(old: Map<ColumnName, HLL>?, new: Map<ColumnName, HLL>?): Map<ColumnName, HLL> =
            when {
                old == null -> new.orEmpty()
                new == null -> old
                else -> {
                    val allCols = old.keys + new.keys
                    allCols.associateWith { col ->
                        val o = old[col]
                        val n = new[col]
                        when {
                            o == null -> n!!
                            n == null -> o
                            else -> o.combine(n)
                        }
                    }
                }
            }

        internal fun mergeTables(old: TableMeta?, delta: TableMeta?): TableMeta {
            if (old == null && delta == null) return TableMeta(emptyMap(), 0, emptyMap())
            if (old == null) return delta!!
            if (delta == null) return old
            return TableMeta(
                vecTypes = mergeVecTypes(old.vecTypes, delta.vecTypes),
                rowCount = old.rowCount + delta.rowCount,
                hlls = mergeHlls(old.hlls, delta.hlls)
            )
        }

        internal fun buildTableBlock(
            vecTypes: Map<ColumnName, VectorType>,
            rowCount: Long,
            partitions: List<Partition>,
            hlls: Map<ColumnName, HLL>
        ): TableBlock {
            val schema = Schema(vecTypes.map { (colName, vecType) -> field(colName, vecType) })

            return TableBlock.newBuilder()
                .apply {
                    this.arrowSchema = ByteString.copyFrom(schema.serializeAsMessage())
                    this.rowCount = rowCount
                    putAllColumnNameToHll(hlls.mapValues { (_, hll) -> ByteString.copyFrom(hll.duplicate()) })
                    addAllPartitions(partitions)
                }
                .build()
        }
    }
}
