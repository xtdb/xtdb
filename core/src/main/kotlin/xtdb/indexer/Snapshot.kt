package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.trie.TrieCatalog
import xtdb.util.closeAll
import xtdb.util.safeMap
import xtdb.util.safelyOpening
import java.time.Instant
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.iterator
import xtdb.api.tx.OpenTx

class Snapshot(
    val txBasis: TransactionKey?,
    val trieCatSnap: TrieCatalog.Snap,
    // Each table can have multiple TableSnapshots once dual-slot LiveTable lands;
    // for now the list is always size 0 or 1, but readers must iterate.
    private val tableSnaps: Map<TableRef, List<TableSnapshot>>,
    val tableInfo: Map<TableRef, Set<ColumnName>>,
) : AutoCloseable {
    interface Source {
        fun openSnapshot(minSystemTime: Instant?): Snapshot
    }

    fun table(table: TableRef): List<TableSnapshot> = tableSnaps[table].orEmpty()
    val tables get() = tableSnaps.keys

    fun columnType(table: TableRef, column: ColumnName): VectorType? {
        val snaps = table(table)
        if (snaps.isEmpty()) return null
        return mergeTypes(snaps.map { it.columnType(column) })
    }

    val allColumnTypes by lazy { tableSnaps.mapValues { (_, snaps) -> snaps.mergeTypes() } }

    /** The frozen per-table trie-cat state for [table], for downstream `current-tries` planning. */
    fun trieTableState(table: TableRef): Any? = trieCatSnap.tableState(table)

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
        private fun TableCatalog.buildTableInfo(
            allColumnTypes: Map<TableRef, Map<ColumnName, VectorType>>?
        ): Map<TableRef, Set<ColumnName>> {
            val tableInfo = HashMap<TableRef, MutableSet<ColumnName>>()

            for ((table, types) in types)
                tableInfo.getOrPut(table) { mutableSetOf() }.addAll(types.keys)

            for ((table, types) in allColumnTypes.orEmpty())
                tableInfo.getOrPut(table) { mutableSetOf() }.addAll(types.keys)

            return tableInfo
        }

        // Precedence, bottom→top (later layers win): durable live tables ⊕ in-flight staged txs
        // (oldest→newest) ⊕ the resolving tx's own writes. External snapshots pass neither and see
        // durable only (strict visibility); a resolving tx passes the in-flight staged txs it must read
        // behind plus itself (read-your-writes across the batch).
        @JvmStatic
        fun open(
            al: BufferAllocator, tableCat: TableCatalog,
            trieCatalog: TrieCatalog, liveIndex: LiveIndex,
            resolvedTxs: List<ResolvedTx> = emptyList(),
            ownTx: OpenTx? = null,
        ): Snapshot = safelyOpening {
            val trieCatSnap = trieCatalog.snapshot()

            val liveIndexSnaps = openAll {
                liveIndex.tableRefs
                    .mapNotNull { liveIndex.table(it) }

                    // Skip live-tables already covered by a published L0 — they'd duplicate the
                    // L0's data. The watermark is monotonic, so once L0_N exists we drop the
                    // live-table-N entry from this snapshot for good (its rows are now in L0_N).
                    .filter { it.blockIdx > trieCatSnap.l0MaxBlockIdx(it.table) }

                    .safeMap { TableSnapshot.open(al, it) }
            }

            val stagedTables = resolvedTxs.flatMap { it.allTables }

            val stagedSnaps = openAll {
                stagedTables
                    .safeMap { it.openSnapshot(al) }
                    .filterNotNull()
            }

            val ownSnaps = openAll {
                ownTx?.tables?.safeMap { TableSnapshot.openTx(al, it.value) }?.filterNotNull() ?: emptyList()
            }

            val byTable = (liveIndexSnaps + stagedSnaps + ownSnaps).groupBy { it.table }

            // tableInfo drives base-table resolution — an unresolved table throws `Table not found`. It
            // must carry every staged table's declared columns *including* 0-row ones (e.g. `CREATE TABLE`),
            // which openSnapshot drops from `byTable` (empty relation), so a tx resolving behind a freshly
            // created empty table in the same batch still sees it exists.
            val colTypes = LinkedHashMap<TableRef, MutableMap<ColumnName, VectorType>>()
            for ((table, snaps) in byTable) colTypes.getOrPut(table) { LinkedHashMap() }.putAll(snaps.mergeTypes())
            for (t in stagedTables) colTypes.getOrPut(t.ref) { LinkedHashMap() }.putAll(t.columnTypes)

            val tableInfo = tableCat.buildTableInfo(colTypes)

            Snapshot(ownTx?.txKey ?: liveIndex.latestCompletedTx, trieCatSnap, byTable, tableInfo)
        }
    }
}
