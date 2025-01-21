package xtdb.api.log

import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.CompletableFuture

typealias TxId = Long

interface TxLog {
    interface Subscriber {
        fun accept(t: Record)
    }

    fun interface Subscription : AutoCloseable

    fun appendTx(record: ByteBuffer): CompletableFuture<TxId>
    fun readTxs(afterTxId: TxId?, limit: Int): List<Record>
    fun subscribeTxs(afterTxId: TxId?, subscriber: Subscriber): Subscription

    fun latestSubmittedTxId(): TxId

    class Record(val txId: TxId, val timestamp: Instant, val record: ByteBuffer)
}
