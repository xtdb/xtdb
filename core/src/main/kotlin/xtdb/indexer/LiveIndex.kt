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
import xtdb.trie.ColumnName
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

    @Volatile
    private var sharedSnap: xtdb.indexer.Snapshot? = null
    private val snapLock = StampedLock()
    private val snapRefCounter = RefCounter()
    private val rowCounter = RowCounter()

    @JvmField
    val logLimit = indexerConfig.logLimit

    @JvmField
    val pageLimit = indexerConfig.pageLimit

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

        sharedSnap = Snapshot.open(allocator, tableCatalog, this, openTx = null)

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
                    val liveTable =
                        tables.getOrPut(tableRef) { LiveTable(allocator, tableRef, rowCounter, liveTrieFactory) }
                    liveTable.importData(tableTx.txRelation)
                }
            }

            latestCompletedTx = openTx.txKey

            refreshSnap()
        } finally {
            snapLock.unlock(stamp)
        }
    }


    fun importTx(resolvedTx: ReplicaMessage.ResolvedTx) {
        val txKey = TransactionKey(resolvedTx.txId, resolvedTx.systemTime)
        val stamp = snapLock.writeLock()
        try {
            for ((schemaAndTable, ipcBytes) in resolvedTx.tableData) {
                val tableRef = TableRef.parse(dbName, schemaAndTable)
                val liveTable =
                    this@LiveIndex.tables.getOrPut(tableRef) {
                        LiveTable(
                            allocator,
                            tableRef,
                            rowCounter,
                            liveTrieFactory
                        )
                    }

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

    override fun openSnapshot(): Snapshot {
        val stamp = snapLock.readLock()
        try {
            return sharedSnap!!.also { it.retain() }
        } finally {
            snapLock.unlock(stamp)
        }
    }

    fun openSnapshot(openTx: OpenTx) = Snapshot.open(allocator, tableCatalog, this, openTx)

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
