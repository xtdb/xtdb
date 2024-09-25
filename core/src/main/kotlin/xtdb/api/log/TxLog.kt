package xtdb.api.log

import xtdb.api.TransactionKey
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

interface TxLog {
    fun appendTx(record: ByteBuffer): CompletableFuture<TransactionKey>
    fun readTxs(afterTxId: Long?, limit: Int): List<Record>
    fun subscribeTxs(afterTxId: Long?, subscriber: Subscriber)

    class Record(val txKey: TransactionKey, val record: ByteBuffer)

    interface Subscriber {
        fun onSubscribe(closeHook: AutoCloseable)
        
        fun accept(t: Record)
    }
}
