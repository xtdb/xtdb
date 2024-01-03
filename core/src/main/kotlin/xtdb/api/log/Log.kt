package xtdb.api.log

import xtdb.api.TransactionKey
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

data class LogRecord(val txKey: TransactionKey, val record: ByteBuffer)

interface LogSubscriber {
    fun onSubscribe(closeHook: AutoCloseable)
    fun acceptRecord(record: LogRecord)
}

interface Log {
    fun appendRecord(record: ByteBuffer): CompletableFuture<TransactionKey>
    fun readRecords(afterTxId: Long?, limit: Int): List<LogRecord>
    fun subscribe(afterTxId: Long?, subscriber: LogSubscriber)
}
