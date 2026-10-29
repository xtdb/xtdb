package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import xtdb.catalog.Catalog
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.api.TableRef
import xtdb.table.Oid
import xtdb.trie.ColumnName
import xtdb.trie.MemoryHashTrie
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
 * combining pinned data with a catalog read live at call time. The catalog is one capture, the segments another
 * — see [open].
 */
class Snapshot(
    val txBasis: TransactionKey?,
    val catalogState: Catalog.State,
    // A table already has several of these on a transaction snapshot - the live-index segment, one
    // per staged tx that touched it, and the tx's own writes. Those are all tx-visibility layers,
    // so an external snapshot still sees one segment per table. Dual-slot LiveTable (#4495) is what
    // changes that, adding a second *durable* slot for as long as block N is uploading.
    private val tableSnaps: Map<TableRef, List<TableSnapshot>>,
    // Live tables whose rows a published L0 already covers — see [open] for why their types outlive
    // their rows. A type map rather than the TableSnapshot itself, so the excluded rows stay
    // unreachable from here.
    private val supersededLiveTypes: Map<TableRef, Map<ColumnName, VectorType>>,
    val tableInfo: Map<TableRef, Set<ColumnName>>,
) : AutoCloseable {

    val trieCatSnap: TrieCatalog.State get() = catalogState.trieCat
    val tableCatSnap: TableCatalog.State get() = catalogState.tableCat

    /**
     * Where [table]'s files live, as of this snapshot — so the slugs a scan reads by are the ones recorded
     * in the block whose trie state it planned against.
     */
    fun slug(table: TableRef) = tableCatSnap.slug(table)

    /** Each table's oid, whether or not a block has recorded it yet. */
    val tableOids: Map<TableRef, Oid> get() = tableCatSnap.entries.associate { it.table to it.oid }

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
        val superseded = supersededLiveTypes[table]
        // Hoisted for the same reason TableSnapshot and TableMeta hold theirs: constant for a fixed map,
        // and this loop asks once per column. Doubles as the no-superseded-table answer, since a table
        // that never had one contributes the bottom too.
        val supersededAbsent = superseded?.let { VectorType.absentContribution(it) } ?: VectorType.Nothing

        return cols.associateWith { col ->
            mergeTypes(
                liveSnaps.map { it.contributedType(col) }
                        + (superseded?.get(col) ?: supersededAbsent)
                        + tableCatSnap.contributedType(table, col)
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
        private fun TableCatalog.State.buildTableInfo(
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
            al: BufferAllocator, catalog: Catalog, liveIndex: LiveIndex,
            resolvedTxs: List<ResolvedTx> = emptyList(),
            ownTx: OpenTx? = null,
        ): Snapshot = safelyOpening {
            // Before the live tables: the caller holds off the next block roll, so a block adopted after this
            // capture still has its live table here to be read.
            val catalogState = catalog.current
            val trieCatSnap = catalogState.trieCat

            // Live-tables already covered by a published L0 contribute no *rows* — they'd duplicate the
            // L0's data. The watermark is monotonic, so once L0_N exists we drop the live-table-N entry
            // for good (its rows are now in L0_N).
            //
            // Their *types* we keep. Adopting a block publishes its L0 and its types in one catalog write,
            // but an L0 can also arrive on its own — the `TriesAdded` re-published for multi-writer nodes —
            // and until its block's types reach the catalog, dropping the live half outright would leave
            // nobody able to answer, declaring a narrower type than the rows about to be scanned. The
            // write path keeps the live table alive until the catalog has the types (`nextBlock` runs after
            // the adoption); this keeps the read path inside it. See #5873.
            val (liveTables, supersededTables) = liveIndex.tableRefs
                .mapNotNull { liveIndex.table(it) }
                .partition { it.blockIdx > trieCatSnap.l0MaxBlockIdx(it.table) }

            val supersededLiveTypes = supersededTables.associate { it.table to it.relation.logRelTypes.orEmpty() }

            val stagedTables = resolvedTxs.flatMap { it.allTables }

            // One segment per table, newest wins — these views subsume rather than complement, so scanning
            // more than one of them would count a row once per view able to see it. A staged tx's rows are
            // the committed rows plus its own, and the resolving tx's are those plus its own again, every
            // one of them over the table's own relation. Newest is whatever writes last here, which is what
            // makes the resolver's queue order load-bearing.
            val newestRows = LinkedHashMap<TableRef, Pair<RelationReader, MemoryHashTrie>>()
            for (lt in liveTables) newestRows[lt.table] = lt.relation to lt.trie
            for (staged in stagedTables) newestRows[staged.ref] = staged.liveTable.relation to staged.liveTable.trie
            ownTx?.tables?.forEach { (ref, tableTx) -> newestRows[ref] = tableTx.tx.relation to tableTx.tx.trie }

            val byTable =
                openAll { newestRows.entries.safeMap { (ref, rows) -> TableSnapshot.open(al, ref, rows.first, rows.second) } }
                    .groupBy { it.table }

            // tableInfo drives base-table resolution — an unresolved table throws `Table not found`. It
            // must carry every staged table's declared columns *including* 0-row ones (e.g. `CREATE TABLE`),
            // whose segment declares none — column types are read off the `put` leg, which a table nothing
            // has written to hasn't got — so a tx resolving behind a freshly created empty table in the
            // same batch still sees it exists.
            val liveColumnNames = LinkedHashMap<TableRef, MutableSet<ColumnName>>()
            for ((table, snaps) in byTable)
                snaps.flatMapTo(liveColumnNames.getOrPut(table) { linkedSetOf() }) { it.types.keys }
            for (t in stagedTables) liveColumnNames.getOrPut(t.ref) { linkedSetOf() }.addAll(t.columnTypes.keys)
            for ((table, types) in supersededLiveTypes) liveColumnNames.getOrPut(table) { linkedSetOf() }.addAll(types.keys)

            val tableInfo = catalogState.tableCat.buildTableInfo(liveColumnNames)

            Snapshot(
                ownTx?.txKey ?: liveIndex.latestCompletedTx, catalogState,
                byTable, supersededLiveTypes, tableInfo
            )
        }
    }
}
