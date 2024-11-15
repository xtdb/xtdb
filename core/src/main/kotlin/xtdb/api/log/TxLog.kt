package xtdb.api.log

import xtdb.api.TransactionKey
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

typealias TxId = Long

interface TxLog {
    fun appendTx(record: ByteBuffer): CompletableFuture<TxId>
    fun readTxs(afterTxId: TxId?, limit: Int): List<Record>
    fun subscribeTxs(afterTxId: TxId?, subscriber: Subscriber)

    class Record(val txKey: TransactionKey, val record: ByteBuffer)

    interface Subscriber {
        fun onSubscribe(closeHook: AutoCloseable)
        
        fun accept(t: Record)
    }
}
