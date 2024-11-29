package xtdb.api.log

import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.CompletableFuture

typealias TxId = Long

interface TxLog {
    fun appendTx(record: ByteBuffer): CompletableFuture<TxId>
    fun readTxs(afterTxId: TxId?, limit: Int): List<Record>
    fun subscribeTxs(afterTxId: TxId?, subscriber: Subscriber)

    fun latestSubmittedTxId(): TxId

    class Record(val txId: TxId, val timestamp: Instant, val record: ByteBuffer)

    interface Subscriber {
        fun onSubscribe(closeHook: AutoCloseable)
        
        fun accept(t: Record)
    }
}
