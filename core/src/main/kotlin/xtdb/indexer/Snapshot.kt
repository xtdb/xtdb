package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.TransactionKey
import xtdb.arrow.MergeTypes.Companion.mergeTypes
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog
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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.iterator
import xtdb.api.tx.OpenTx

/**
 * A frozen view of **one partition** at a point in time — see [DatabaseSnapshot] for the whole database.
 *
 * It pins both catalogs, and a [LiveSnap] of the in-memory segments, so a reader is not combining pinned data with
 * a catalog read live at call time.
 *
 * Which live segments it reads rows from follows from its trie catalog: a live table whose block a published L0
 * already covers contributes its types but not its rows.
 */
class Snapshot internal constructor(
    val trieCatSnap: TrieCatalog.State,
    val tableCatSnap: TableCatalog.State,
    private val live: LiveSnap,
) : AutoCloseable {

    val txBasis: TransactionKey? get() = live.txBasis

    /** Column *names* only - [tableInfo] answers which columns a table has; [columnTypes] answers their types. */
    val tableInfo: Map<TableRef, Set<ColumnName>> by lazy {
        val tableInfo = HashMap<TableRef, MutableSet<ColumnName>>()

        for ((table, types) in tableCatSnap.types)
            tableInfo.getOrPut(table) { mutableSetOf() }.addAll(types.keys)

        for ((table, colNames) in live.columnNames)
            tableInfo.getOrPut(table) { mutableSetOf() }.addAll(colNames)

        tableInfo
    }

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

    private fun LiveSnap.Durable.superseded() = blockIdx <= trieCatSnap.l0MaxBlockIdx(snap.table)

    // One segment per table, newest wins — these views subsume rather than complement, so scanning
    // more than one of them would count a row once per view able to see it.
    private val tableSnaps: Map<TableRef, TableSnapshot> by lazy {
        val byTable = LinkedHashMap<TableRef, TableSnapshot>()
        for ((table, durable) in live.durable) if (!durable.superseded()) byTable[table] = durable.snap
        byTable.putAll(live.layered)
        byTable
    }

    // Live tables whose rows a published L0 already covers. A type map rather than the TableSnapshot itself,
    // so the excluded rows stay unreachable from here.
    private val supersededLiveTypes: Map<TableRef, Map<ColumnName, VectorType>> by lazy {
        live.durable.filterValues { it.superseded() }.mapValues { (_, durable) -> durable.snap.types }
    }

    fun table(table: TableRef): List<TableSnapshot> = listOfNotNull(tableSnaps[table])
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

    private val closed = AtomicBoolean(false)

    override fun close() {
        if (closed.compareAndSet(false, true)) live.release()
    }

    companion object {
        // Precedence, bottom→top (later layers win): durable live tables ⊕ in-flight staged txs
        // (oldest→newest) ⊕ the resolving tx's own writes. External snapshots pass neither and see
        // durable only (strict visibility); a resolving tx passes the in-flight staged txs it must read
        // behind plus itself (read-your-writes across the batch).
        internal fun open(
            al: BufferAllocator, trieCat: TrieCatalog.State, tableCat: TableCatalog.State, live: LiveIndex.State,
            resolvedTxs: List<ResolvedTx>, ownTx: OpenTx?,
        ): Snapshot = Snapshot(trieCat, tableCat, LiveSnap.open(al, live, resolvedTxs, ownTx))
    }
}

/**
 * The live half of a [Snapshot]: a slice of every live table, and the one count every snapshot over them shares —
 * the slices are closed when it reaches zero.
 */
internal class LiveSnap private constructor(
    val txBasis: TransactionKey?,
    // Every durable live table, whether or not a published L0 covers its block: which ones a snapshot reads rows
    // from is its trie catalog's to say.
    internal val durable: Map<TableRef, Durable>,
    // The newest of a transaction's staged predecessors' segments and its own, per table it has written; empty for
    // an external snapshot.
    internal val layered: Map<TableRef, TableSnapshot>,
    // Every column a live segment declares, plus each staged table's declared columns — see [open].
    internal val columnNames: Map<TableRef, Set<ColumnName>>,
) {
    internal class Durable(val blockIdx: Long, val snap: TableSnapshot)

    private val refCount = AtomicInteger(1)

    fun retain() {
        if (0 == refCount.getAndIncrement()) throw IllegalStateException("snapshot closed")
    }

    fun release() {
        if (0 == refCount.decrementAndGet()) (durable.values.map { it.snap } + layered.values).closeAll()
    }

    companion object {
        internal fun open(
            al: BufferAllocator, live: LiveIndex.State,
            resolvedTxs: List<ResolvedTx>, ownTx: OpenTx?,
        ): LiveSnap = safelyOpening {
            val liveTables = live.tables.values.toList()
            val durable = liveTables.zip(openAll { liveTables.safeMap { TableSnapshot.open(al, it) } })
                .associate { (lt, snap) -> lt.table to Durable(lt.blockIdx, snap) }

            val stagedTables = resolvedTxs.flatMap { it.allTables }

            // A staged tx's rows are the committed rows plus its own, and the resolving tx's are those plus its own
            // again, every one of them over the table's own relation. Newest is whatever writes last here, which is
            // what makes the resolver's queue order load-bearing.
            val newestLayered = LinkedHashMap<TableRef, Pair<RelationReader, MemoryHashTrie>>()
            for (staged in stagedTables) newestLayered[staged.ref] = staged.liveTable.relation to staged.liveTable.trie
            ownTx?.tables?.forEach { (ref, tableTx) -> newestLayered[ref] = tableTx.tx.relation to tableTx.tx.trie }

            val layered = openAll { newestLayered.entries.safeMap { (ref, rows) -> TableSnapshot.open(al, ref, rows.first, rows.second) } }
                .associateBy { it.table }

            // Snapshot.tableInfo, built from these, drives base-table resolution — an unresolved table throws `Table not found`. It
            // must carry every staged table's declared columns *including* 0-row ones (e.g. `CREATE TABLE`),
            // whose segment declares none — column types are read off the `put` leg, which a table nothing
            // has written to hasn't got — so a tx resolving behind a freshly created empty table in the
            // same batch still sees it exists.
            //
            // A durable table's columns are listed whether or not an L0 covers it — a covered table's types are
            // still read — and a layered segment's are a superset of the durable one it is written over.
            val liveColumnNames = LinkedHashMap<TableRef, MutableSet<ColumnName>>()
            for ((table, d) in durable) liveColumnNames.getOrPut(table) { linkedSetOf() }.addAll(d.snap.types.keys)
            for ((table, snap) in layered) liveColumnNames.getOrPut(table) { linkedSetOf() }.addAll(snap.types.keys)
            for (t in stagedTables) liveColumnNames.getOrPut(t.ref) { linkedSetOf() }.addAll(t.columnTypes.keys)

            LiveSnap(ownTx?.txKey ?: live.latestCompletedTx, durable, layered, liveColumnNames)
        }
    }
}
