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
        fun liveTable(table: TableRef): LiveTable.Snapshot?
        val liveTables: Iterable<TableRef>
    }

    interface Tx : AutoCloseable {
        fun liveTable(table: TableRef): LiveTable.Tx
        val liveTables: Iterable<Map.Entry<TableRef, LiveTable.Tx>>
        fun openSnapshot(): Snapshot

        fun commit()
        fun abort()
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

        openLiveIdxSnap(tables).closeOnCatch { liveIdxSnap ->
            sharedSnap = Snapshot(latestCompletedTx, liveIdxSnap, buildSchema(liveIdxSnap, tableCatalog))
        }

        oldSnap?.close()
    }

    fun liveTable(table: TableRef): LiveTable? = tables[table]
    val liveTables: Iterable<TableRef> get() = tables.keys

    fun startTx(txKey: TransactionKey): Tx {
        val tableTxs = HashMap<TableRef, LiveTable.Tx>()
        val thisIdx = this

        return object : Tx {
            override fun liveTable(table: TableRef): LiveTable.Tx =
                tableTxs.computeIfAbsent(table) { t ->
                    val existing = thisIdx.liveTable(t)
                    val liveTable = existing ?: LiveTable(allocator, t, rowCounter, liveTrieFactory)
                    liveTable.startTx(txKey, existing == null)
                }

            override val liveTables: Iterable<Map.Entry<TableRef, LiveTable.Tx>> get() = tableTxs.entries

            override fun commit() {
                val stamp = snapLock.writeLock()
                try {
                    for ((table, tx) in tableTxs)
                        tables[table] = tx.commit()

                    latestCompletedTx = txKey

                    thisIdx.refreshSnap()
                } finally {
                    snapLock.unlock(stamp)
                }
            }

            override fun abort() {
                for (tx in tableTxs.values)
                    tx.abort()
                latestCompletedTx = txKey
            }

            override fun openSnapshot(): Snapshot {
                val snaps = HashMap<TableRef, LiveTable.Snapshot>()
                snaps.closeAllOnCatch {
                    for ((table, tx) in tableTxs)
                        snaps[table] = tx.openSnapshot()

                    for ((table, liveTable) in tables)
                        snaps.computeIfAbsent(table) { liveTable.openSnapshot() }

                    return object : Snapshot {
                        override val allColumnTypes get() = snaps.mapValues { (_, snap) -> snap.types }
                        override fun liveTable(table: TableRef) = snaps[table]
                        override val liveTables get() = snaps.keys
                        override fun close() = snaps.values.closeAll()
                    }
                }
            }

            override fun close() = tableTxs.values.closeAll()
        }
    }

    fun importTx(resolvedTx: ReplicaMessage.ResolvedTx) {
        val txKey = TransactionKey(resolvedTx.txId, resolvedTx.systemTime)
        val stamp = snapLock.writeLock()
        try {
            for ((schemaAndTable, ipcBytes) in resolvedTx.tableData) {
                val tableRef = TableRef.parse(dbName, schemaAndTable)
                val liveTable =
                    tables.getOrPut(tableRef) { LiveTable(allocator, tableRef, rowCounter, liveTrieFactory) }

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

    fun finishBlock(bp: BufferPool, blockIdx: BlockIndex) = tables.finishBlock(bp, blockIdx)

    private val skipTxsLogged = AtomicBoolean(false)

    fun nextBlock() {
        rowCounter.nextBlock()

        val stamp = snapLock.writeLock()
        try {
            tables.values.closeAll()
            tables.clear()

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
        tables.values.closeAll()
        if (!snapRefCounter.tryClose(Duration.ofMinutes(1)))
            LOG.warn("Failed to shut down live-index after 60s due to outstanding watermarks.")
        else
            allocator.close()
    }

    companion object {
        private fun openLiveIdxSnap(tables: Map<TableRef, LiveTable>): Snapshot {
            HashMap<TableRef, LiveTable.Snapshot>().closeAllOnCatch { snaps ->
                for ((table, liveTable) in tables)
                    snaps[table] = liveTable.openSnapshot()

                return object : Snapshot {
                    override val allColumnTypes get() = snaps.mapValues { (_, snap) -> snap.types }
                    override fun liveTable(table: TableRef) = snaps[table]
                    override val liveTables get() = snaps.keys
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
