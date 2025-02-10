package xtdb.indexer

import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.TransactionKey
import xtdb.trie.BlockIndex
import xtdb.trie.MemoryHashTrie
import xtdb.vector.IRelationWriter
import xtdb.vector.IVectorWriter
import xtdb.vector.RelationReader
import java.nio.ByteBuffer

interface LiveTable : AutoCloseable {
    interface Watermark : AutoCloseable {
        fun columnField(col: String): Field
        fun columnFields(): Map<String, Field>
        fun liveRelation(): RelationReader
        fun liveTrie(): MemoryHashTrie
    }

    interface Tx : AutoCloseable{
        fun openWatermark(): Watermark
        val docWriter: IVectorWriter
        val liveRelation: IRelationWriter

        fun logPut(iid: ByteBuffer, validFrom: Long, validTo: Long, writeDocFun: Runnable)
        fun logDelete(iid: ByteBuffer, validFrom: Long, validTo: Long)
        fun logErase(iid: ByteBuffer)

        fun commit(): LiveTable
        fun abort()
    }

    val liveRelation: IRelationWriter

    fun startTx(txKey: TransactionKey, newLiveTable: Boolean): Tx
    fun openWatermark(): Watermark
    fun finishBlock(blockIdx: BlockIndex): Collection<Map.Entry<String, *>>
}