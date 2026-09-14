package xtdb.indexer

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
 * field. [resolveOversized] is the read-side counterpart, and resolves a payload whoever wrote it.
 *
 * [LeaderDriver.uploadBlock] appends its own `BlockUploaded` inside [BlockUploader] rather than through
 * here, so that is the one replica-log message this does not cover.
 */
internal class OffloadingLeaderDriver(
    private val delegate: LeaderDriver,
    partitionStorage: PartitionStorage,
    private val partitionState: PartitionState,
) : LeaderDriver by delegate {

    private val bufferPool = partitionStorage.bufferPool

    override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata =
        try {
            delegate.appendToReplica(msg)
        } catch (e: Log.MessageTooLargeException) {
            val ref = ReplicaMessage.OversizedMessage(
                Storage.VERSION, bufferPool.epoch,
                (partitionState.tableCatalogOrNull?.currentBlockIndex ?: -1) + 1,
                randomUUID().toString(),
                termId = msg.termId,
            )

            bufferPool.putObject(ref.path, ByteBuffer.wrap(msg.encode()))

            delegate.appendToReplica(ref)
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
