package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
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
    private val tableSnaps: Map<TableRef, TableSnapshot>,
    val tableInfo: Map<TableRef, Set<ColumnName>>
) : AutoCloseable {
    interface Source {
        fun openSnapshot(): Snapshot
    }

    fun table(table: TableRef) = tableSnaps[table]
    val tables get() = tableSnaps.keys
    fun columnType(table: TableRef, column: ColumnName) = table(table)?.columnType(column)
    val allColumnTypes get() = tableSnaps.mapValues { (_, snap) -> snap.types }

    private val refCount = AtomicInteger(1)

    fun retain() {
        if (0 == refCount.getAndIncrement()) throw IllegalStateException("snapshot closed")
    }

    override fun close() {
        if (0 == refCount.decrementAndGet()) tableSnaps.closeAll()
    }

    companion object {
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
            HashMap<TableRef, TableSnapshot>().closeAllOnCatch { snaps ->
                if (openTx != null)
                    for ((tableRef, tableTx) in openTx.tables) {
                        snaps[tableRef] = TableSnapshot.open(al, liveIndex.table(tableRef), tableTx)
                    }

                for (tableRef in liveIndex.tableRefs)
                    snaps.computeIfAbsent(tableRef) {
                        TableSnapshot.open(al, liveIndex.table(tableRef), tableTx = null)
                    }

                val tableInfo = buildTableInfo(snaps.mapValues { (_, snap) -> snap.types }, tableCat)

                Snapshot(openTx?.txKey ?: liveIndex.latestCompletedTx, snaps, tableInfo)
            }
    }
}
