package xtdb.indexer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.nio.ByteBuffer
import java.time.Instant
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import xtdb.api.log.Log
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.BlockUploaded
import xtdb.api.log.SourceMessage
import xtdb.api.storage.Storage
import xtdb.catalog.BlockCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.log.proto.TrieDetails
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.logger

private val LOG = BlockUploader::class.logger

// Cap concurrent per-table block-metadata uploads to bound the heap held by in-flight
// `TableBlock.toByteArray()` buffers. S3's default HTTP client caps concurrency at 50, so
// parallelism beyond this would queue inside the SDK with the `byte[]`s still pinned on heap.
private const val MAX_CONCURRENT_BLOCK_UPLOADS = 16
private val blockUploadDispatcher = IO.limitedParallelism(MAX_CONCURRENT_BLOCK_UPLOADS, "block-upload")

class BlockUploader(
    dbStorage: DatabaseStorage,
    dbState: DatabaseState,
    private val compactor: Compactor.ForDatabase,
    private val dbCatalog: Database.Catalog?,
    private val meterRegistry: MeterRegistry?,
) {
    private val sourceLog = dbStorage.sourceLog
    private val bufferPool = dbStorage.bufferPool
    private val liveIndex = dbState.liveIndex
    private val blockCatalog = dbState.blockCatalog
    private val trieCatalog = dbState.trieCatalog
    private val tableCatalog = dbState.tableCatalog
    private val blockUploadTimer: Timer? = meterRegistry?.let {
        Timer.builder("block.upload.timer")
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .register(it)
    }

    suspend fun uploadBlock(
        replicaProducer: Log.AtomicProducer<ReplicaMessage>, boundaryReplicaMsgId: MessageId, boundary: BlockBoundary,
    ): MessageId {
        val latestProcessedMsgId = boundary.latestProcessedMsgId
        val blockIdx = boundary.blockIndex
        LOG.debug("finishing block: 'b${blockIdx.asLexHex}'...")
        val timer = meterRegistry?.let { Timer.start(it) }

        val finishedBlocks = liveIndex.finishBlock(bufferPool, blockIdx)

        val addedTries =
            finishedBlocks.map { (table, fb) ->
                val trieDetails = TrieDetails.newBuilder()
                    .setTableName(table.schemaAndTable)
                    .setTrieKey(fb.trieKey)
                    .setDataFileSize(fb.dataFileSize)
                    .also { fb.trieMetadata.let { tm -> it.setTrieMetadata(tm) } }
                    .build()

                // NOTE: side-effect here.
                trieCatalog.addTries(table, listOf(trieDetails), Instant.now())

                trieDetails
            }

        // Publish L0 tries to source log so that all nodes (including concurrent MW nodes)
        // see the L0 before any compaction L1C on the source log — see #5395.
        sourceLog.appendMessage(SourceMessage.TriesAdded(Storage.VERSION, bufferPool.epoch, addedTries))

        val allTables = finishedBlocks.keys + blockCatalog.allTables
        val tablePartitions = allTables.associateWith { trieCatalog.getPartitions(it) }

        val tableBlocks = tableCatalog.finishBlock(blockCatalog.currentBlockIndex, finishedBlocks, tablePartitions)

        runBlocking {
            tableBlocks.map { (table, tableBlock) ->
                async(blockUploadDispatcher) {
                    val path = BlockCatalog.tableBlockPath(table, blockIdx)
                    bufferPool.putObject(path, ByteBuffer.wrap(tableBlock.toByteArray()))
                }
            }.awaitAll()
        }

        val secondaryDatabasesForBlock = dbCatalog?.serialisedSecondaryDatabases

        val externalSourceToken = boundary.externalSourceToken

        val block = blockCatalog.buildBlock(
            blockIdx, liveIndex.latestCompletedTx, latestProcessedMsgId,
            boundaryReplicaMsgId, tableBlocks.keys, secondaryDatabasesForBlock,
            externalSourceToken
        )

        bufferPool.putObject(BlockCatalog.blockFilePath(blockIdx), ByteBuffer.wrap(block.toByteArray()))
        blockCatalog.refresh(block)

        // Now signal followers that the block is available.
        @OptIn(ExperimentalCoroutinesApi::class)
        val uploadedMsgId = replicaProducer.withTx { tx ->
            tx.appendMessage(
                BlockUploaded(
                    Storage.VERSION, bufferPool.epoch,
                    blockIdx, latestProcessedMsgId,
                    addedTries, externalSourceToken
                )
            )
        }.getCompleted().msgId

        LOG.debug("block uploaded b${blockIdx.asLexHex}: source=$latestProcessedMsgId, replica=$uploadedMsgId")

        blockUploadTimer?.let { timer?.stop(it) }
        liveIndex.nextBlock()
        compactor.signalBlock()
        LOG.debug("finished block: 'b${blockIdx.asLexHex}'.")

        return uploadedMsgId
    }
}
