package xtdb.indexer

import xtdb.api.DatabaseName
import xtdb.api.log.ReplicaMessage.BlockBoundary
import xtdb.api.tx.ExternalSourceToken
import xtdb.database.PartitionState
import xtdb.types.MessageId

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
    partitionState: PartitionState,
    private val dbName: DatabaseName,
    private val leaderTerm: Long,
    private val replicaAppender: ReplicaLogAppender,
    private val blockUploader: BlockUploader,
) {
    private val liveIndex = partitionState.liveIndex
    private val tableCatalog = partitionState.tableCatalog

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
     * The live index holds exactly this block's txs by now, in log order; snapshotting it, appending the
     * matching `BlockUploaded` and rolling the index all happen inside [BlockUploader.uploadBlock].
     */
    suspend fun upload(boundaryMsgId: MessageId, boundary: BlockBoundary) {
        blockState = Uploading
        blockUploader.uploadBlock(boundaryMsgId, leaderTerm, boundary)
        // Straight after the upload, so a demote landing here hands on nothing: the block is done.
        blockState = Filling(0)
    }
}
