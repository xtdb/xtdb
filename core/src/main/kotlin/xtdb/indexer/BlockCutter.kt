package xtdb.indexer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import xtdb.api.DatabaseName
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.log.ReplicaMessage.BlockUploaded
import xtdb.api.log.SourceMessage
import xtdb.api.storage.Storage
import xtdb.api.tx.ExternalSourceToken
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.log.proto.TrieDetails
import xtdb.types.MessageId
import xtdb.util.StringUtil.asLexHex
import xtdb.util.debug
import xtdb.util.logger
import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

private val LOG = BlockCutter::class.logger

private const val MAX_CONCURRENT_BLOCK_UPLOADS = 16

/**
 * Where a leader term is in the block it is filling: [Filling] → [Cut] → [Uploading] → [Filling].
 *
 * Resolution is armed only in [Filling] — see [acceptingResolution] — so no tx can interleave between a
 * boundary and its upload, which is what keeps the follower's bounded pending-block buffer empty.
 *
 * The term's coroutine is both the sole writer and the sole reader, so nothing here is published across
 * coroutines.
 */
internal class BlockCutter(
    partitionStorage: PartitionStorage,
    partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val leaderTerm: Long,
    private val replicaAppender: ReplicaLogAppender,
    // Both seams, because the two messages of a cut need different things. The boundary goes through the
    // pump, to stay ordered behind the txs already queued there; the `BlockUploaded` goes direct, because
    // it has to be durable before this node rolls its own index past the block — see [uploadBlock].
    private val logsDriver: LogProcessor.LogsDriver,
    private val compactor: Compactor.ForDatabase,
    private val dbCatalog: Database.Catalog?,
    private val meterRegistry: MeterRegistry?,
    // The database's, not this term's: it backs a gauge registered once alongside it, and a gauge keeps
    // the state object it was registered with — so a per-term one would be dropped (#5867).
    private val lastUploadEpochSeconds: AtomicLong,
    // The database's too, for the trailing post below, which outlives the upload deliberately.
    private val scope: CoroutineScope,
    ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    // Caps the per-table block-file uploads below, bounding the heap held by their in-flight
    // `TableBlock.toByteArray()` buffers. S3's default HTTP client caps concurrency at 50, so
    // parallelism beyond this would queue inside the SDK with the `byte[]`s still pinned on heap.
    private val uploadDispatcher = ioDispatcher.limitedParallelism(MAX_CONCURRENT_BLOCK_UPLOADS, "block-upload")

    private val sourceLog = partitionStorage.sourceLog
    private val bufferPool = partitionStorage.bufferPool
    private val liveIndex = partitionState.liveIndex
    private val trieCatalog = partitionState.trieCatalog
    private val tableCatalog = partitionState.tableCatalog

    private val blockUploadTimer: Timer? = meterRegistry?.let {
        Timer.builder("block.upload.timer")
            .publishPercentiles(0.75, 0.85, 0.95, 0.98, 0.99, 0.999)
            .register(it)
    }

    private sealed interface BlockState

    /**
     * Accumulating rows towards the cut, [rows] of them so far.
     *
     * The boundary is cut off this rather than `liveIndex.isFull()`, which lags — it reflects only APPLIED
     * (consume-back) txs. Seeded from the rows already applied into the open block, because a new leader
     * inherits a partially-filled block from replay and must cut it where the old leader would have, or
     * block sizes drift across restarts (the #5817 stop/start off-by-one).
     */
    private class Filling(val rows: Long) : BlockState

    /** The boundary is queued for append, and has not been read back yet. */
    private data object Cut : BlockState

    /** The boundary has been applied and the upload is in flight. */
    private data object Uploading : BlockState

    private var blockState: BlockState = Filling(liveIndex.blockRowCount)

    // From the live index, not the node config: the two agree in production, but they are one value and the
    // live index is what owns the block being filled.
    private val rowsPerBlock = liveIndex.rowsPerBlock

    /**
     * Whether this term will take resolution work right now.
     *
     * False for the length of a block cut, so nothing interleaves between the boundary and its upload —
     * which is what keeps the follower's bounded pending-block buffer empty.
     */
    val acceptingResolution get() = blockState is Filling

    /**
     * Whether the block being filled has reached [rowsPerBlock], and so wants [cut].
     *
     * An empty block is never full, whatever the threshold. Without that, a `rowsPerBlock` of 0 leaves
     * the term cutting an empty block, re-opening an empty one and cutting that — live-locked, never
     * arming resolution again.
     */
    val isFull get() = (blockState as? Filling)?.let { it.rows > 0 && it.rows >= rowsPerBlock } == true

    /** Account [rows] more towards the block being filled. */
    fun addRows(rows: Long) {
        blockState = when (val state = blockState) {
            is Filling -> Filling(state.rows + rows)

            // Only reachable from clauses the term arms in Filling alone, so getting here means the
            // arm-set and this state have come apart.
            Cut, Uploading -> error("[$dbName] tx resolved during a block cut")
        }
    }

    fun addRows(resolvedTx: ResolvedTx) {
        addRows(resolvedTx.allTables.sumOf { it.relation.rowCount.toLong() })
    }

    /**
     * Cut the block: inject a boundary covering the source log up to [latestProcessedMsgId] and the
     * external source up to [extToken], and pause resolution until that boundary has been read back and
     * uploaded.
     *
     * Both watermarks are the resolve side's, so the caller supplies them — this reads only the block
     * index the boundary follows on from. Queued through the append pump rather than appended directly, so
     * it lands in resolution order: after this block's txs and before the next block's.
     */
    suspend fun cut(latestProcessedMsgId: MessageId, extToken: ExternalSourceToken?) {
        val boundary = BlockBoundary(
            (tableCatalog.currentBlockIndex ?: -1) + 1, latestProcessedMsgId, extToken,
            termId = leaderTerm
        )
        replicaAppender.append(ControlItem(boundary))
        blockState = Cut
    }

    /**
     * Close the block that [boundary] cut, read back at [boundaryMsgId], and re-arm resolution behind it.
     *
     * The live index holds exactly this block's txs by now, in log order, so this snapshots it into block
     * files, queues the matching `BlockUploaded` and rolls the index.
     */
    suspend fun upload(boundaryMsgId: MessageId, boundary: BlockBoundary) {
        blockState = Uploading
        uploadBlock(boundaryMsgId, boundary)
        // Straight after the upload, so a demote landing here hands on nothing: the block is done.
        blockState = Filling(0)
    }

    private suspend fun uploadBlock(boundaryReplicaMsgId: MessageId, boundary: BlockBoundary) {
        val latestProcessedMsgId = boundary.latestProcessedMsgId
        val blockIdx = boundary.blockIndex
        LOG.debug("finishing block: 'b${blockIdx.asLexHex}'...")
        val timer = meterRegistry?.let { Timer.start(it) }

        val finishedBlocks = liveIndex.finishBlock(bufferPool, blockIdx)

        // A table staged with no rows has no trie — see LiveTable.FinishedBlock.writtenTrie. It still
        // reaches the table catalog below, so its declared columns survive.
        val addedTries =
            finishedBlocks.mapNotNull { (table, fb) ->
                val writtenTrie = fb.writtenTrie ?: return@mapNotNull null

                val trieDetails = TrieDetails.newBuilder()
                    .setTableName(table.schemaAndTable)
                    .setTrieKey(writtenTrie.trieKey)
                    .setDataFileSize(writtenTrie.dataFileSize)
                    .also { it.setTrieMetadata(writtenTrie.trieMetadata) }
                    .build()

                // NOTE: side-effect here.
                trieCatalog.addTries(table, listOf(trieDetails), Instant.now())

                trieDetails
            }

        val allTables = finishedBlocks.keys + tableCatalog.allTables
        val tablePartitions = allTables.associateWith { trieCatalog.getPartitions(it) }

        val tableBlocks = tableCatalog.finishBlock(finishedBlocks, tablePartitions)

        // A table new in this block has already written its L0 trie by now, under the slug its LiveTable was
        // created with — the same one minted here, because both resolve through `State.slug`.
        val entries = tableCatalog.resolveTables(tableBlocks.keys).associateBy { it.table }

        coroutineScope {
            tableBlocks.forEach { (table, tableBlock) ->
                launch(uploadDispatcher) {
                    val path = TableCatalog.tableBlockPath(entries.getValue(table).slug, blockIdx)
                    bufferPool.putObject(path, ByteBuffer.wrap(tableBlock.toByteArray()))
                }
            }
        }

        val secondaryDatabasesForBlock = dbCatalog?.serialisedSecondaryDatabases

        val externalSourceToken = boundary.externalSourceToken

        val block = tableCatalog.buildBlock(
            blockIdx, liveIndex.latestCompletedTx, latestProcessedMsgId,
            boundaryReplicaMsgId, entries.values, secondaryDatabasesForBlock,
            externalSourceToken, leaderTerm
        )

        bufferPool.putObject(TableCatalog.blockFilePath(blockIdx), ByteBuffer.wrap(block.toByteArray()))
        tableCatalog.refresh(block)
        lastUploadEpochSeconds.set(Instant.now().epochSecond)

        // Awaited, and not through the append pump: every follower is buffering behind the boundary until
        // this lands, and `nextBlock` below commits this node to the block. Queued instead, a term ending
        // in between would drop it — leaving this node past the block with nothing on the log to release
        // the followers, and unable to re-cut it because its own catalog has moved on.
        val uploadedMsgId = logsDriver.appendToReplica(
            BlockUploaded(
                Storage.VERSION, bufferPool.epoch,
                blockIdx, latestProcessedMsgId,
                addedTries, externalSourceToken,
                // This term's, not the boundary's: a promotion finishes the block the previous leader
                // cut, and a leader confirms a write only on reading it back at its own term.
                termId = leaderTerm,
            )
        ).msgId

        LOG.debug("block uploaded b${blockIdx.asLexHex}: source=$latestProcessedMsgId, replica=$uploadedMsgId")

        blockUploadTimer?.let { timer?.stop(it) }
        liveIndex.nextBlock()

        // Publish L0 tries to the source log so that all nodes — including multi-writer nodes running
        // concurrently with a single-writer leader — see the L0 before any compaction L1C on the source
        // log (see #5395). Once we drop support for multi-writer clusters running concurrently with
        // single-writer, this source-log post is no longer required and can be removed.
        //
        // Both off the persister coroutine, in one launch: this runs on the source log's sole consumer, so
        // appending inline self-deadlocks when the source-log buffer saturates at a block boundary — the
        // consumer would wait on room only it can make. The launch lets the upload return so the persister
        // drains and the append's emit finds room; running signalBlock after it in the same coroutine keeps
        // compaction's L1C strictly behind the L0 without a separate join.
        scope.launch {
            sourceLog.appendMessage(SourceMessage.TriesAdded(Storage.VERSION, bufferPool.epoch, addedTries))
            compactor.signalBlock()
        }
        LOG.debug("finished block: 'b${blockIdx.asLexHex}'.")
    }
}
