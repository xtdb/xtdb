package xtdb.indexer

import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import java.time.Instant

/**
 * Records what a term appends instead of writing it, so a test that only cares what reached the replica
 * log needs neither a log, a tail, nor a delay to observe it.
 *
 * A message recorded here is never read back, so anything waiting on a record's *application* — a
 * `ResolvedTx` reaching the live index, an `executeTx` handle, the watchers' watermarks — stays pending.
 * Those tests want the real log.
 */
internal class RecordingLogsDriver : LogProcessor.LogsDriver {

    val appended = mutableListOf<ReplicaMessage>()

    override suspend fun appendToReplica(msg: ReplicaMessage): Log.MessageMetadata {
        appended += msg
        return Log.MessageMetadata(0, appended.size - 1L, Instant.now())
    }

    override suspend fun requestFlushBlock(expectedBlockIdx: Long) =
        error("nothing under test asks the source log for a flush")
}
