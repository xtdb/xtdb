@file:UseSerializers(DurationSerde::class, PathWithEnvVarSerde::class)
package xtdb.api.log

import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.TransactionKey
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

interface Log : AutoCloseable {
    fun appendRecord(record: ByteBuffer): CompletableFuture<TransactionKey>
    fun readRecords(afterTxId: Long?, limit: Int): List<Record>
    fun subscribe(afterTxId: Long?, subscriber: Subscriber)

    /**
     * @suppress
     */
    override fun close() {
    }

    interface Factory {
        fun openLog(): Log
    }

    data class Record(val txKey: TransactionKey, val record: ByteBuffer)

    interface Subscriber {
        fun onSubscribe(closeHook: AutoCloseable)
        fun acceptRecord(record: Record)
    }
}
