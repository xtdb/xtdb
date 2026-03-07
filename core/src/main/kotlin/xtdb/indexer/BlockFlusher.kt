package xtdb.indexer

import xtdb.api.log.MessageId
import xtdb.catalog.BlockCatalog
import java.time.Duration
import java.time.Instant

data class BlockFlusher(
    val flushTimeout: Duration,
    var lastFlushCheck: Instant,
    var previousBlockTxId: MessageId,
    var flushedTxId: MessageId
) {
    constructor(
        flushTimeout: Duration, blockCatalog: BlockCatalog
    ) : this(
        flushTimeout, Instant.now(),
        previousBlockTxId = blockCatalog.latestCompletedTx?.txId ?: -1,
        flushedTxId = -1
    )

    fun checkBlockTimeout(now: Instant, currentBlockTxId: MessageId, latestCompletedTxId: MessageId): Boolean =
        when {
            lastFlushCheck + flushTimeout >= now || flushedTxId == latestCompletedTxId -> false

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

    fun checkBlockTimeout(blockCatalog: BlockCatalog, liveIndex: LiveIndex) =
        checkBlockTimeout(
            Instant.now(),
            currentBlockTxId = blockCatalog.latestCompletedTx?.txId ?: -1,
            latestCompletedTxId = liveIndex.latestCompletedTx?.txId ?: -1
        )
}