package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.arrow.MergeTypes.Companion.joinContributions
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog
import xtdb.api.TableRef
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

/**
 * A frozen view of **one partition** at a point in time — see [DatabaseSnapshot] for the whole database.
 *
 * It pins the in-memory segments, the trie catalog and the historical table metadata, so a reader is not
 * combining pinned data with a catalog read live at call time. The two catalog captures are not atomic with
 * respect to each other, though — see [open].
 */
class Snapshot(
    val txBasis: TransactionKey?,
    val trieCatSnap: TrieCatalog.Snap,
    val tableCatSnap: TableCatalog.Snap,
    // A table already has several of these on a transaction snapshot - the live-index segment, one
    // per staged tx that touched it, and the tx's own writes. Those are all tx-visibility layers,
    // so an external snapshot still sees one segment per table. Dual-slot LiveTable (#4495) is what
    // changes that, adding a second *durable* slot for as long as block N is uploading.
    private val tableSnaps: Map<TableRef, List<TableSnapshot>>,
    val tableInfo: Map<TableRef, Set<ColumnName>>,
) : AutoCloseable {
    interface Source {
        fun openSnapshot(minSystemTime: Instant?): Snapshot
    }

    fun table(table: TableRef): List<TableSnapshot> = tableSnaps[table].orEmpty()
    val tables get() = tableSnaps.keys

    /** The frozen per-table trie-cat state for [table], for downstream `current-tries` planning. */
    fun trieTableState(table: TableRef): Any? = trieCatSnap.tableState(table)

    /**
     * The declared type of each of [cols] in [table] - the historical half joined with the live half.
     *
     * Takes the columns it is asked about rather than returning whatever it finds. A column can belong to the
     * table while being absent from one half — recorded historically, missing from a live segment — and that
     * half still has an answer: its rows read null for it. Indexing into a found-columns map would make that
     * a lookup miss, and the caller would have to invent what a miss means.
     */
    fun columnTypes(table: TableRef, cols: Iterable<ColumnName>): Map<ColumnName, VectorType> {
        val liveSnaps = table(table)

        return cols.associateWith { col ->
            joinContributions(
                liveSnaps.map { it.contributedType(col) } + tableCatSnap.contributedType(table, col)
            )
        }
    }

    /** [tableInfo] is the one enumeration of a table's columns - typing whatever it lists keeps them agreeing. */
    fun columnTypes(table: TableRef): Map<ColumnName, VectorType> =
        columnTypes(table, tableInfo[table].orEmpty())

    val allColumnTypes: Map<TableRef, Map<ColumnName, VectorType>> by lazy {
        tableInfo.keys.associateWith { columnTypes(it) }
    }

    private val refCount = AtomicInteger(1)

    fun retain() {
        if (0 == refCount.getAndIncrement()) throw IllegalStateException("snapshot closed")
    }

    override fun close() {
        if (0 == refCount.decrementAndGet()) tableSnaps.values.flatten().closeAll()
    }

    companion object {
        /** Column *names* only - [tableInfo] answers which columns a table has; [columnTypes] answers their types. */
        @JvmStatic
        private fun TableCatalog.Snap.buildTableInfo(
            liveColumnNames: Map<TableRef, Set<ColumnName>>
        ): Map<TableRef, Set<ColumnName>> {
            val tableInfo = HashMap<TableRef, MutableSet<ColumnName>>()

            for ((table, types) in types)
                tableInfo.getOrPut(table) { mutableSetOf() }.addAll(types.keys)

            for ((table, colNames) in liveColumnNames)
                tableInfo.getOrPut(table) { mutableSetOf() }.addAll(colNames)

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
            val liveColumnNames = LinkedHashMap<TableRef, MutableSet<ColumnName>>()
            for ((table, snaps) in byTable)
                snaps.flatMapTo(liveColumnNames.getOrPut(table) { linkedSetOf() }) { it.types.keys }
            for (t in stagedTables) liveColumnNames.getOrPut(t.ref) { linkedSetOf() }.addAll(t.columnTypes.keys)

            // Captured last, deliberately: a block lands as `addTries` then `finishBlock`, and a table is
            // invisible to both halves only if the trie-cat capture falls after the first and this one
            // before the second, so the wider the gap the narrower that window. Pinning still widens the
            // exposure overall - consumers used to read the catalog live at query time, strictly later
            // than here. See #5873.
            val tableCatSnap = tableCat.snapshot()

            val tableInfo = tableCatSnap.buildTableInfo(liveColumnNames)

            Snapshot(ownTx?.txKey ?: liveIndex.latestCompletedTx, trieCatSnap, tableCatSnap, byTable, tableInfo)
        }
    }
}
