package xtdb

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import org.apache.arrow.memory.BufferAllocator
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.IndexerConfig
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.block.proto.Block
import xtdb.catalog.TableCatalog
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.indexer.LiveIndex
import xtdb.storage.BufferPool
import java.time.InstantSource

/** An [InMemoryLog] on the system clock at epoch 0 — for wherever the log itself is not what is under test. */
fun <M> inMemoryLog() = InMemoryLog<M>(InstantSource.system(), 0)

/**
 * One partition's [PartitionStorage] and [PartitionState], over real catalogs and a real [LiveIndex].
 *
 * Owns the live index and nothing else: [bufferPool] and the logs outlive it, as they do in production
 * where both belong to the Database — so a test sharing either across nodes passes the same instance to
 * each and closes it itself.
 *
 * [block] starts the table catalog on an already-flushed block, as a node restarting from storage does.
 */
class TestPartition(
    allocator: BufferAllocator,
    val bufferPool: BufferPool,
    val sourceLog: Log<SourceMessage> = inMemoryLog(),
    val replicaLog: Log<ReplicaMessage> = inMemoryLog(),
    block: Block? = null,
    indexerConfig: IndexerConfig = IndexerConfig(),
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    val tableCatalog = TableCatalog(bufferPool, block)
    val trieCatalog = createTrieCatalog()
    val liveIndex = LiveIndex.open(allocator, tableCatalog, trieCatalog, indexerConfig, ioDispatcher)

    val state = PartitionState(tableCatalog, trieCatalog, liveIndex)
    val storage = PartitionStorage(DatabaseLogs(sourceLog, replicaLog), bufferPool, null)

    override fun close() = state.close()
}
