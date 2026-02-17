package xtdb.indexer

import xtdb.api.log.MessageId
import java.util.concurrent.CompletableFuture

interface LogProcessor : AutoCloseable {
    val latestProcessedMsgId: MessageId
    val latestProcessedOffset: Long
    val latestSubmittedMsgId: MessageId
    val ingestionError: Throwable?

    fun awaitAsync(msgId: MessageId): CompletableFuture<*>

    override fun close()
}
