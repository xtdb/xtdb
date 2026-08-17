package xtdb.indexer

import xtdb.types.MessageId
import xtdb.catalog.BlockCatalog
import java.time.Duration
import java.time.Instant

/**
 * Decides when the flush timeout should cut a block, from the leader's poll loop.
 *
 * A database quiet enough never to reach `rowsPerBlock` cuts a block every [flushTimeout] regardless
 * of whether anything has been written since the last one. That is deliberate: an idle database's
 * block anchors are what keep a Kafka log's `retention.ms` from expiring the offsets a restart seeks
 * back to, so they have to keep moving whether or not there is data behind them (#5778). A block with
 * no transactions in it writes no tries — only the block file and its table blocks — so the cost is
 * bounded and the compactor sees nothing.
 */
data class BlockFlusher(
    val flushTimeout: Duration,
    var lastFlushCheck: Instant,
    var previousBlockTxId: MessageId,
) {
    constructor(
        flushTimeout: Duration, blockCatalog: BlockCatalog
    ) : this(
        flushTimeout, Instant.now(),
        previousBlockTxId = blockCatalog.latestCompletedTx?.txId ?: -1
    )

    fun checkBlockTimeout(now: Instant, currentBlockTxId: MessageId): Boolean =
        when {
            lastFlushCheck + flushTimeout >= now -> false

            // A block landed by some other route since we last looked — the row-count path, or an
            // explicit FlushBlock. Re-arm rather than cutting a second one straight after it.
            currentBlockTxId != previousBlockTxId -> {
                lastFlushCheck = now
                previousBlockTxId = currentBlockTxId
                false
            }

            else -> {
                lastFlushCheck = now
                true
            }
        }

    fun checkBlockTimeout(blockCatalog: BlockCatalog) =
        checkBlockTimeout(Instant.now(), currentBlockTxId = blockCatalog.latestCompletedTx?.txId ?: -1)
}
