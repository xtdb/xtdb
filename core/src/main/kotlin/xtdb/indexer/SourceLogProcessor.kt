package xtdb.indexer

import xtdb.api.log.Log

/**
 * Subscribes to the source log and delegates all records to the ReplicaLogProcessor.
 * This passthrough establishes subscription ownership on the source side;
 * in a subsequent commit, this will resolve txs before passing to the replica.
 */
class SourceLogProcessor(
    private val replicaProcessor: ReplicaLogProcessor,
) : Log.Subscriber {

    override fun processRecords(records: List<Log.Record>) {
        replicaProcessor.processRecords(records)
    }
}
