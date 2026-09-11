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
import xtdb.block.proto.Block
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.Database
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.log.proto.TrieDetails
import xtdb.types.LogTimestamp
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
 * Where a leader term is in the block it is filling: [Filling] → [Cut] → [Uploading] → [Filling], the
 * last step driven by reading our own `BlockUploaded` back rather than by finishing the upload.
 *
 * Resolution is armed only in [Filling] — see [acceptingResolution] — so no tx can interleave between a
 * boundary and its upload. That is what keeps the follower's bounded pending-block buffer empty, and now
 * also what keeps anything from applying inside [Uploading], where the live index holds a block already
 * snapshotted into L0.
 *
 * The term's coroutine is the sole writer and, [pendingBlock] apart, the sole reader; a demotion reads
 * that one only after joining the term, which is the edge that publishes it.
 */
internal class BlockCutter(
    partitionStorage: PartitionStorage,
    private val partitionState: PartitionState,
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

    /**
     * The block's files are in storage and its `BlockUploaded` is appended, but this node has not read
     * that message back — so its catalog and live index are still on the block.
     *
     * The [block] is here because the produce step is the only thing that can build it and the adopt runs
     * a whole log round-trip later; everything else the adopt needs it takes from the record.
     */
    private class Uploading(val pendingBlock: PendingBlock, val block: Block) : BlockState

    private var blockState: BlockState = Filling(liveIndex.blockRowCount)

    /**
     * The block this term has produced and is waiting to read back, holding whatever arrived behind its
     * boundary — null unless an upload is in flight.
     *
     * A demotion hands this to the next follower, which then closes the block on the same message this
     * term was waiting for. Without the handover the block would be left open on a node with nothing to
     * close it: the upload is not stale, because [closeBlock] is what moves the catalog past it, so it
     * would reach the follower's `processRecord` with no block to match and stop the partition's tail.
     */
    val pendingBlock get() = (blockState as? Uploading)?.pendingBlock

    /** Whether [msg] is the upload this term is waiting for — the follower's test, applied to our own write. */
    fun closes(msg: BlockUploaded) =
        (blockState as? Uploading)?.pendingBlock?.let {
            msg.blockIndex == it.blockIdx
                    && msg.storageVersion == Storage.VERSION && msg.storageEpoch == bufferPool.epoch
        } == true

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
            Cut, is Uploading -> error("[$dbName] tx resolved during a block cut")
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
     * Produce the block that [pendingBlock]'s boundary cut: snapshot the live index into block files and
     * append the matching `BlockUploaded`.
     *
     * The live index holds exactly this block's txs by now, in log order — which is what the block-cut
     * pause buys. What this does *not* do is adopt the result: the catalog refresh and the index roll
     * wait for [closeBlock], on reading that message back.
     *
     * Deferring them is what leaves a term dying here recoverable. Its catalog has not moved past the
     * block, so the next leader can produce it again — where `TableCatalog.buildBlock` refuses a second
     * attempt at an index the catalog already holds.
     */
    suspend fun upload(pendingBlock: PendingBlock) {
        blockState = uploadBlock(pendingBlock)
    }

    /**
     * Adopt the block [msg] confirms, re-open the one behind it, and hand back what was held meanwhile.
     *
     * The caller applies those held records only after this returns. They belong to the block opening
     * here, and a tx applied between `finishBlock` and `nextBlock` would land in live tables already
     * snapshotted into L0 and about to be cleared — so the rows would be written nowhere.
     */
    fun closeBlock(msg: BlockUploaded, logTimestamp: LogTimestamp): PendingBlock {
        val uploading = blockState as? Uploading
            ?: error("[$dbName] BlockUploaded b${msg.blockIndex.asLexHex} arrived with no block in flight")

        partitionState.adoptBlock(uploading.block, msg.tries, logTimestamp)

        // Publish L0 tries to the source log so that all nodes — including multi-writer nodes running
        // concurrently with a single-writer leader — see the L0 before any compaction L1C on the source
        // log (see #5395). Once we drop support for multi-writer clusters running concurrently with
        // single-writer, this source-log post is no longer required and can be removed.
        //
        // Both off the persister coroutine, in one launch: this runs on the source log's sole consumer, so
        // appending inline self-deadlocks when the source-log buffer saturates at a block boundary — the
        // consumer would wait on room only it can make. The launch lets the close return so the persister
        // drains and the append's emit finds room; running signalBlock after it in the same coroutine keeps
        // compaction's L1C strictly behind the L0 without a separate join.
        scope.launch {
            sourceLog.appendMessage(
                SourceMessage.TriesAdded(Storage.VERSION, bufferPool.epoch, msg.tries)
            )
            compactor.signalBlock()
        }

        blockState = Filling(0)

        LOG.debug("finished block: 'b${msg.blockIndex.asLexHex}'.")

        return uploading.pendingBlock
    }

    private suspend fun uploadBlock(pendingBlock: PendingBlock): Uploading {
        val boundary = pendingBlock.boundaryMessage
        val boundaryReplicaMsgId = pendingBlock.boundaryMsgId
        val latestProcessedMsgId = boundary.latestProcessedMsgId
        val blockIdx = boundary.blockIndex
        LOG.debug("finishing block: 'b${blockIdx.asLexHex}'...")
        val timer = meterRegistry?.let { Timer.start(it) }

        val finishedBlocks = liveIndex.finishBlock(bufferPool, blockIdx)

        // One timestamp for the whole block rather than one per table: it dates the supersession these
        // tries cause, and they all supersede as of the same block.
        val triesAsOf = Instant.now()

        // One trie per table that took rows — `writtenTrie` is singular — and none at all for a table
        // staged with no rows. That table still reaches the table catalog below, so its declared columns
        // survive.
        val addedTriesByTable =
            finishedBlocks.mapNotNull { (table, fb) ->
                fb.writtenTrie?.let { writtenTrie ->
                    table to TrieDetails.newBuilder()
                        .setTableName(table.schemaAndTable)
                        .setTrieKey(writtenTrie.trieKey)
                        .setDataFileSize(writtenTrie.dataFileSize)
                        .also { it.setTrieMetadata(writtenTrie.trieMetadata) }
                        .build()
                }
            }.toMap()

        val addedTries = addedTriesByTable.values.toList()

        // The layout these tries imply rather than the one the catalog holds: the block records the
        // partitions as of itself, and the add below is what the catalog will agree with afterwards.
        val allTables = finishedBlocks.keys + tableCatalog.allTables
        val tablePartitions = allTables.associateWith { table ->
            trieCatalog.withPartitions(table, listOfNotNull(addedTriesByTable[table]), triesAsOf)
        }

        val tableBlocks = tableCatalog.buildTableBlocks(finishedBlocks, tablePartitions)

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
            externalSourceToken,
            // not leaderTerm - #6059
            boundary.termId
        )

        bufferPool.putObject(TableCatalog.blockFilePath(blockIdx), ByteBuffer.wrap(block.toByteArray()))
        lastUploadEpochSeconds.set(Instant.now().epochSecond)

        // Awaited, and not through the append pump: this is the message the whole cluster is waiting on
        // to close the block, this node now included. Queued, a term ending in between would drop it —
        // the pump's shutdown discards whatever it still holds. Awaited, a failure to append reaches the
        // term instead, which leaves the boundary unapplied for the next role to pick up and re-produce.
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

        return Uploading(pendingBlock, block)
    }
}
