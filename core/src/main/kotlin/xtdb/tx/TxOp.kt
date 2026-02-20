package xtdb.tx

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.table.SchemaName
import xtdb.table.TableName
import xtdb.table.TableRef
import java.time.Instant

sealed interface TxOp : AutoCloseable {

    fun openSlice(al: BufferAllocator): TxOp

    data class PutDocs(
        val schema: SchemaName, val table: TableName,
        val validFrom: Instant? = null, val validTo: Instant? = null,
        val docs: RelationReader
    ) : TxOp {
        override fun openSlice(al: BufferAllocator): TxOp = copy(docs = docs.openSlice(al))
        override fun close() = docs.close()
    }

    data class PatchDocs(
        val schema: SchemaName, val table: TableName,
        val validFrom: Instant? = null, val validTo: Instant? = null,
        val patches: RelationReader
    ) : TxOp {
        override fun openSlice(al: BufferAllocator): TxOp = copy(patches = patches.openSlice(al))
        override fun close() = patches.close()
    }

    data class DeleteDocs(
        val schema: SchemaName, val table: TableName,
        val validFrom: Instant? = null, val validTo: Instant? = null,
        val ids: VectorReader
    ) : TxOp {
        override fun openSlice(al: BufferAllocator): TxOp = copy(ids = ids.openSlice(al))
        override fun close() = ids.close()
    }

    data class EraseDocs(val schema: SchemaName, val table: TableName, val ids: VectorReader) : TxOp {
        override fun openSlice(al: BufferAllocator): TxOp = copy(ids = ids.openSlice(al))
        override fun close() = ids.close()
    }

    data class Sql(val sql: String, val args: RelationReader? = null) : TxOp {
        override fun openSlice(al: BufferAllocator): TxOp = copy(args = args?.openSlice(al))

        override fun close() {
            args?.close()
        }
    }

    /**
     * SQL with pre-serialized Arrow IPC stream bytes for args.
     * Used by FlightSQL prepared statement updates where args arrive as bytes over the wire.
     */
    data class SqlBytes(val sql: String, val argBytes: ByteArray) : TxOp {
        override fun openSlice(al: BufferAllocator): TxOp = this
        override fun close() = Unit
    }
}