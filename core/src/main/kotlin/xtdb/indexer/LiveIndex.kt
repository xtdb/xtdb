package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.IndexerConfig
import xtdb.api.TransactionKey
import xtdb.api.log.ReplicaMessage
import xtdb.arrow.Relation
import xtdb.arrow.VectorType
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.indexer.LiveTable.Companion.finishBlock
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.BlockIndex
import xtdb.trie.MemoryHashTrie
import xtdb.util.RowCounter
import xtdb.util.RefCounter
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.logger
import xtdb.util.safelyOpening
import xtdb.util.warn
import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import java.time.Duration
import java.util.HashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.StampedLock

private val LOG = LiveIndex::class.logger

class LiveIndex private constructor(
    private val allocator: BufferAllocator,
    private val tableCatalog: TableCatalog,
    private val dbName: String,
    @Volatile var latestCompletedTx: TransactionKey?,
    indexerConfig: IndexerConfig,
) : Snapshot.Source, AutoCloseable {

    private val tables = HashMap<TableRef, LiveTable>()
    @Volatile private var sharedSnap: xtdb.indexer.Snapshot? = null
    private val snapLock = StampedLock()
    private val snapRefCounter = RefCounter()
    private val rowCounter = RowCounter()

    interface Snapshot : AutoCloseable {
        val allColumnTypes: Map<TableRef, Map<String, VectorType>>
        fun table(table: TableRef): TableSnapshot?
        val tables: Iterable<TableRef>
    }

    @JvmField val logLimit = indexerConfig.logLimit
    @JvmField val pageLimit = indexerConfig.pageLimit
    private val rowsPerBlock = indexerConfig.rowsPerBlock
    private val skipTxs = indexerConfig.skipTxs

    private val liveTrieFactory = LiveTable.LiveTrieFactory { iidVec ->
        MemoryHashTrie.builder(iidVec)
            .setLogLimit(logLimit.toInt())
            .setPageLimit(pageLimit.toInt())
            .build()
    }

    private fun refreshSnap() {
        val oldSnap = sharedSnap

        openLiveIdxSnap(allocator, this@LiveIndex.tables).closeOnCatch { liveIdxSnap ->
            sharedSnap = Snapshot(latestCompletedTx, liveIdxSnap, buildSchema(liveIdxSnap, tableCatalog))
        }

        oldSnap?.close()
    }

    fun table(table: TableRef): LiveTable? = this@LiveIndex.tables[table]
    val tableRefs: Iterable<TableRef> get() = this@LiveIndex.tables.keys

    fun startTx(txKey: TransactionKey) = OpenTx(allocator, txKey)

    fun commitTx(openTx: OpenTx) {
        val stamp = snapLock.writeLock()
        try {
            for ((tableRef, tableTx) in openTx.tables) {
                if (tableTx.txRelation.rowCount > 0) {
                    val liveTable = tables.getOrPut(tableRef) { LiveTable(allocator, tableRef, rowCounter, liveTrieFactory) }
                    liveTable.importData(tableTx.txRelation)
                }
            }

            latestCompletedTx = openTx.txKey

            refreshSnap()
        } finally {
            snapLock.unlock(stamp)
        }
    }

    fun openSnapshot(openTx: OpenTx): Snapshot {
        val snaps = HashMap<TableRef, TableSnapshot>()
        snaps.closeAllOnCatch {
            for ((tableRef, tableTx) in openTx.tables) {
                val liveTable = tables.getOrPut(tableRef) { LiveTable(allocator, tableRef, rowCounter, liveTrieFactory) }
                snaps[tableRef] = TableSnapshot.open(allocator, liveTable, tableTx)
            }

            for ((table, liveTable) in tables)
                snaps.computeIfAbsent(table) { TableSnapshot.open(allocator, liveTable, tableTx = null) }

            return object : Snapshot {
                override val allColumnTypes get() = snaps.mapValues { (_, snap) -> snap.types }
                override fun table(table: TableRef) = snaps[table]
                override val tables get() = snaps.keys
                override fun close() = snaps.values.closeAll()
            }
        }
    }

    fun importTx(resolvedTx: ReplicaMessage.ResolvedTx) {
        val txKey = TransactionKey(resolvedTx.txId, resolvedTx.systemTime)
        val stamp = snapLock.writeLock()
        try {
            for ((schemaAndTable, ipcBytes) in resolvedTx.tableData) {
                val tableRef = TableRef.parse(dbName, schemaAndTable)
                val liveTable =
                    this@LiveIndex.tables.getOrPut(tableRef) { LiveTable(allocator, tableRef, rowCounter, liveTrieFactory) }

                Relation.StreamLoader(allocator, Channels.newChannel(ByteArrayInputStream(ipcBytes))).use { loader ->
                    Relation(allocator, loader.schema).use { rel ->
                        loader.loadNextPage(rel)
                        liveTable.importData(rel)
                    }
                }
            }

            latestCompletedTx = txKey

            refreshSnap()
        } finally {
            snapLock.unlock(stamp)
        }
    }

    override fun openSnapshot(): xtdb.indexer.Snapshot {
        val stamp = snapLock.readLock()
        try {
            return sharedSnap!!.also { it.retain() }
        } finally {
            snapLock.unlock(stamp)
        }
    }

    fun isFull() = rowCounter.blockRowCount >= rowsPerBlock

    fun blockMetadata(): Map<TableRef, LiveTable.BlockMetadata> =
        this@LiveIndex.tables.mapNotNull { (table, lt) -> lt.blockMetadata()?.let { table to it } }.toMap()

    fun finishBlock(bp: BufferPool, blockIdx: BlockIndex) = this@LiveIndex.tables.finishBlock(bp, blockIdx)

    private val skipTxsLogged = AtomicBoolean(false)

    fun nextBlock() {
        rowCounter.nextBlock()

        val stamp = snapLock.writeLock()
        try {
            this@LiveIndex.tables.values.closeAll()
            this@LiveIndex.tables.clear()

            refreshSnap()
        } finally {
            snapLock.unlock(stamp)
        }

        if (skipTxs.isNotEmpty() && latestCompletedTx != null
            && latestCompletedTx!!.txId >= skipTxs.last()
            && skipTxsLogged.compareAndSet(false, true)
        ) {
            LOG.warn("All XTDB_SKIP_TXS have been skipped and block has been finished - it is safe to remove the XTDB_SKIP_TXS environment variable.")
        }
    }

    override fun close() {
        sharedSnap?.close()
        this@LiveIndex.tables.values.closeAll()
        if (!snapRefCounter.tryClose(Duration.ofMinutes(1)))
            LOG.warn("Failed to shut down live-index after 60s due to outstanding watermarks.")
        else
            allocator.close()
    }

    companion object {
        private fun openLiveIdxSnap(al: BufferAllocator, tables: Map<TableRef, LiveTable>): Snapshot {
            HashMap<TableRef, TableSnapshot>().closeAllOnCatch { snaps ->
                for ((table, liveTable) in tables)
                    snaps[table] = TableSnapshot.open(al, liveTable, tableTx = null)

                return object : Snapshot {
                    override val allColumnTypes get() = snaps.mapValues { (_, snap) -> snap.types }
                    override fun table(table: TableRef) = snaps[table]
                    override val tables get() = snaps.keys
                    override fun close() = snaps.values.closeAll()
                }
            }
        }

        @JvmStatic
        fun buildSchema(snap: Snapshot?, tableCatalog: TableCatalog): Map<TableRef, Any> {
            val schema = HashMap<TableRef, MutableSet<String>>()

            for ((table, types) in tableCatalog.types)
                schema.getOrPut(table) { mutableSetOf() }.addAll(types.keys)
            if (snap != null)
                for ((table, types) in snap.allColumnTypes)
                    schema.getOrPut(table) { mutableSetOf() }.addAll(types.keys)

            return schema
        }

        @JvmStatic
        @JvmOverloads
        @JvmName("open")
        fun open(
            allocator: BufferAllocator, blockCatalog: BlockCatalog, tableCatalog: TableCatalog,
            dbName: String, indexerConfig: IndexerConfig = IndexerConfig(),
        ) = safelyOpening {
            LiveIndex(
                open { allocator.newChildAllocator("live-index", 0, Long.MAX_VALUE) },
                tableCatalog, dbName,
                blockCatalog.latestCompletedTx,
                indexerConfig,
            ).also { it.refreshSnap() }
        }
    }
}
