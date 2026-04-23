package xtdb.indexer

/**
 * Seam for the commit step of [TxIndexer.indexTx]: apply an [OpenTx] to the live index,
 * publish to the replica log, notify watchers, and handle any per-processor bookkeeping
 * (e.g. block-boundary flushes).
 *
 * The external-source path (via [ExternalSourceProcessor]) and, in time, the internal
 * log-consumer path (via [LeaderLogProcessor]) each implement this; `TxIndexer` itself
 * only owns the per-tx lifecycle around the commit call.
 */
interface TxCommitter {
    suspend fun commit(openTx: OpenTx, result: TxIndexer.TxResult)
}
