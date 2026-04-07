package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE
import xtdb.api.TransactionKey
import xtdb.arrow.Relation
import xtdb.arrow.VectorWriter
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.MemoryHashTrie
import xtdb.trie.Trie
import xtdb.util.closeAll
import java.nio.ByteBuffer
import kotlin.Long.Companion.MAX_VALUE as MAX_LONG
import kotlin.Long.Companion.MIN_VALUE as MIN_LONG

class OpenTx(private val allocator: BufferAllocator, val txKey: TransactionKey) : AutoCloseable {

    val systemFrom = txKey.systemTime.asMicros

    private val tableTxs = HashMap<TableRef, Table>()

    fun table(table: TableRef): Table =
        tableTxs.getOrPut(table) { Table(allocator, systemFrom) }

    val tables: Iterable<Map.Entry<TableRef, Table>> get() = tableTxs.entries

    fun serializeTableData(): Map<String, ByteArray> =
        tableTxs.mapNotNull { (tableRef, tableTx) ->
            tableTx.serializeTxData()?.let { tableRef.schemaAndTable to it }
        }.toMap()

    override fun close() = tableTxs.values.closeAll()

    class Table(
        allocator: BufferAllocator,
        private val systemFrom: Long,
    ) : AutoCloseable {

        val txRelation: Relation = Trie.openLogDataWriter(allocator)

        private val iidVec = txRelation["_iid"]
        private val systemFromVec = txRelation["_system_from"]
        private val validFromVec = txRelation["_valid_from"]
        private val validToVec = txRelation["_valid_to"]
        private val opVec = txRelation["op"]
        private val putVec by lazy(LazyThreadSafetyMode.NONE) { opVec.vectorFor("put", STRUCT_TYPE, false) }
        private val deleteVec = opVec["delete"]
        private val eraseVec = opVec["erase"]

        val docWriter: VectorWriter by lazy(LazyThreadSafetyMode.NONE) { putVec }

        var trie: MemoryHashTrie = MemoryHashTrie.emptyTrie(iidVec)
            private set

        fun logPut(iid: ByteBuffer, validFrom: Long, validTo: Long, writeDocFun: Runnable) {
            val pos = txRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(validFrom)
            validToVec.writeLong(validTo)

            writeDocFun.run()

            txRelation.endRow()

            trie += pos
        }

        fun logDelete(iid: ByteBuffer, validFrom: Long, validTo: Long) {
            val pos = txRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(validFrom)
            validToVec.writeLong(validTo)
            deleteVec.writeNull()
            txRelation.endRow()

            trie += pos
        }

        fun logErase(iid: ByteBuffer) {
            val pos = txRelation.rowCount

            iidVec.writeBytes(iid)
            systemFromVec.writeLong(systemFrom)
            validFromVec.writeLong(MIN_LONG)
            validToVec.writeLong(MAX_LONG)
            eraseVec.writeNull()
            txRelation.endRow()

            trie += pos
        }

        fun serializeTxData(): ByteArray? =
            if (txRelation.rowCount > 0) txRelation.asArrowStream else null

        override fun close() {
            txRelation.close()
        }
    }
}
