package xtdb.indexer

import kotlinx.coroutines.Deferred
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.storage.Storage
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.BufferPool
import java.nio.ByteBuffer
import java.util.UUID.randomUUID

/**
 * Appends to the replica log, putting a message the log declines for its size into the object store instead.
 *
 * The declined message's own encoding is uploaded and a [ReplicaMessage.OversizedMessage] naming it is
 * appended in its place, so this works for any message kind rather than one that was given a by-reference
 * field. [resolveOversized] is the read-side counterpart.
 *
 * Appending through a partition with no buffer pool rethrows, since there is nowhere to put the payload.
 */
internal class OffloadingLogsDriver(
    private val delegate: LogProcessor.LogsDriver,
    private val partitionStorage: PartitionStorage,
    private val partitionState: PartitionState,
) : LogProcessor.LogsDriver {

    override suspend fun requestFlushBlock(expectedBlockIdx: Long) = delegate.requestFlushBlock(expectedBlockIdx)

    override suspend fun enqueueToReplica(msg: ReplicaMessage): Deferred<Log.MessageMetadata> =
        try {
            delegate.enqueueToReplica(msg)
        } catch (e: Log.MessageTooLargeException) {
            val bufferPool = partitionStorage.bufferPoolOrNull ?: throw e

            val ref = ReplicaMessage.OversizedMessage(
                Storage.VERSION, bufferPool.epoch,
                (partitionState.tableCatalogOrNull?.currentBlockIndex ?: -1) + 1,
                randomUUID().toString(),
                termId = msg.termId, termSeq = msg.termSeq,
            )

            bufferPool.putObject(ref.path, ByteBuffer.wrap(msg.encode()))

            delegate.enqueueToReplica(ref)
        }
}

/**
 * The message [record] stands in for, where it is a [ReplicaMessage.OversizedMessage]; [record] unchanged
 * otherwise.
 *
 * Callers MUST resolve before reading anything but `termId` off the message: an offloaded `BlockUploaded`
 * closes a block, and one left unresolved would be buffered behind the block it was meant to close.
 */
internal fun BufferPool.resolveOversized(record: Log.Record<ReplicaMessage>): Log.Record<ReplicaMessage> {
    val msg = record.message
    if (msg !is ReplicaMessage.OversizedMessage) return record

    // Fails rather than skipping, unlike the sibling storage-version guards: those concern artefacts a new
    // epoch makes irrelevant, whereas the payload here may be a committed transaction, and dropping one of
    // those silently diverges this replica from the rest. Migrating storage must carry `oversized/` across.
    check(msg.storageVersion == Storage.VERSION && msg.storageEpoch == epoch) {
        "can't resolve oversized message at ${record.msgId}: " +
                "written at storage v${msg.storageVersion}/e${msg.storageEpoch}, reading v${Storage.VERSION}/e$epoch — " +
                "migrating storage must copy the `oversized/` prefix along with the rest"
    }

    val payload = ReplicaMessage.decode(getByteArray(msg.path))
        ?: error("unreadable oversized message payload at ${msg.path}")

    return Log.Record(record.epoch, record.logOffset, record.logTimestamp, payload)
}
