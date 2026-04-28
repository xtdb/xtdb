package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.iterator

class Snapshot(
    val txBasis: TransactionKey?,
    // Each table can have multiple TableSnapshots once dual-slot LiveTable lands;
    // for now the list is always size 0 or 1, but readers must iterate.
    private val tableSnaps: Map<TableRef, List<TableSnapshot>>,
    val tableInfo: Map<TableRef, Set<ColumnName>>
) : AutoCloseable {
    interface Source {
        fun openSnapshot(): Snapshot
    }

    fun table(table: TableRef): List<TableSnapshot> = tableSnaps[table].orEmpty()
    val tables get() = tableSnaps.keys
    fun columnType(table: TableRef, column: ColumnName): VectorType? {
        val snaps = table(table)
        if (snaps.isEmpty()) return null
        return mergeTypes(snaps.map { it.columnType(column) })
    }
    val allColumnTypes get() = tableSnaps.mapValues { (_, snaps) -> snaps.mergeTypes() }

    private val refCount = AtomicInteger(1)

    fun retain() {
        if (0 == refCount.getAndIncrement()) throw IllegalStateException("snapshot closed")
    }

    override fun close() {
        if (0 == refCount.decrementAndGet()) tableSnaps.values.flatten().closeAll()
    }

    companion object {
        private fun List<TableSnapshot>.mergeTypes(): Map<ColumnName, VectorType> =
            when {
                isEmpty() -> emptyMap()
                size == 1 -> first().types
                else -> {
                    val allCols = flatMapTo(mutableSetOf()) { it.types.keys }
                    allCols.associateWith { col -> mergeTypes(map { it.columnType(col) }) }
                }
            }

        @JvmStatic
        private fun buildTableInfo(
            allColumnTypes: Map<TableRef, Map<ColumnName, VectorType>>?, tableCatalog: TableCatalog
        ): Map<TableRef, Set<ColumnName>> {
            val tableInfo = HashMap<TableRef, MutableSet<ColumnName>>()

            for ((table, types) in tableCatalog.types)
                tableInfo.getOrPut(table) { mutableSetOf() }.addAll(types.keys)

            for ((table, types) in allColumnTypes.orEmpty())
                tableInfo.getOrPut(table) { mutableSetOf() }.addAll(types.keys)

            return tableInfo
        }

        @JvmStatic
        fun open(al: BufferAllocator, tableCat: TableCatalog, liveIndex: LiveIndex, openTx: OpenTx?): Snapshot =
            mutableListOf<TableSnapshot>().closeAllOnCatch { allSnaps ->
                val byTable = HashMap<TableRef, MutableList<TableSnapshot>>()
                val openTxTables: Map<TableRef, OpenTx.Table> =
                    openTx?.tables?.associate { it.key to it.value }.orEmpty()

                fun addSnap(tableRef: TableRef, ts: TableSnapshot) {
                    allSnaps.add(ts)
                    byTable.getOrPut(tableRef) { mutableListOf() }.add(ts)
                }

                for (tableRef in openTxTables.keys + liveIndex.tableRefs) {
                    liveIndex.table(tableRef)?.let { addSnap(tableRef, TableSnapshot.open(al, it)) }
                    openTxTables[tableRef]?.let { tx -> TableSnapshot.openTx(al, tx)?.let { addSnap(tableRef, it) } }
                }

                val tableInfo = buildTableInfo(byTable.mapValues { (_, snaps) -> snaps.mergeTypes() }, tableCat)

                Snapshot(openTx?.txKey ?: liveIndex.latestCompletedTx, byTable, tableInfo)
            }
    }
}
