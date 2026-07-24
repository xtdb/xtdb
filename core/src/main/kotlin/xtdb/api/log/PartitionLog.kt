package xtdb.api.log

import xtdb.types.MessageId

/**
 * A [Log] view bound to a single [partition]: the partition-indexed operations with the index pre-applied,
 * so a per-partition consumer holds its slice of the log without threading the partition through every call.
 *
 * Deliberately not the whole [Log] surface — the database-level operations ([Log.openGroupSubscription],
 * one consumer-group membership per database, and [Log.epoch]) belong to the shared log, reached through
 * DatabaseLogs, not to a single partition's view.
 */
class PartitionLog<M>(private val log: Log<M>, val partition: Int) {
    suspend fun appendMessage(message: M): Log.MessageMetadata = log.appendMessage(message, partition)

    suspend fun tailAll(afterMsgId: MessageId, processor: Log.RecordProcessor<M>) =
        log.tailAll(partition, afterMsgId, processor)
}
