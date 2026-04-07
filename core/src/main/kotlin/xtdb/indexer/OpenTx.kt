package xtdb.indexer

import xtdb.arrow.Relation
import xtdb.arrow.VectorWriter
import xtdb.table.TableRef
import xtdb.trie.MemoryHashTrie
import java.nio.ByteBuffer

interface OpenTx : AutoCloseable {
    fun table(table: TableRef): Table
    val tables: Iterable<Map.Entry<TableRef, Table>>
    fun openSnapshot(): LiveIndex.Snapshot

    interface Table : AutoCloseable {
        val docWriter: VectorWriter

        val txRelation: Relation
        val transientTrie: MemoryHashTrie

        fun openSnapshot(): LiveTable.Snapshot
        fun logPut(iid: ByteBuffer, validFrom: Long, validTo: Long, writeDocFun: Runnable)
        fun logDelete(iid: ByteBuffer, validFrom: Long, validTo: Long)
        fun logErase(iid: ByteBuffer)
        fun serializeTxData(): ByteArray?
    }

    fun commit()
    fun abort()
}