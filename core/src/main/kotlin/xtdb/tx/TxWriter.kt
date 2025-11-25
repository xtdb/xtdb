@file:JvmName("TxWriter")

package xtdb.tx

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.*
import xtdb.arrow.VectorType.Companion.IID
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.LIST_TYPE
import xtdb.arrow.VectorType.Companion.STRUCT_TYPE
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.VAR_BINARY
import xtdb.arrow.VectorType.Companion.just
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.unionOf
import xtdb.error.Incorrect
import xtdb.table.SchemaName
import xtdb.table.TableName
import xtdb.util.asIid
import java.time.Instant
import java.time.ZoneId

private val txSchema = schema(
    "tx-ops" ofType listTypeOf(unionOf()),
    "system-time" ofType maybe(INSTANT),
    "default-tz" ofType UTF8,
    "user" ofType maybe(UTF8),
    "user-metadata" ofType maybe(STRUCT_TYPE)
)

private val FORBIDDEN_SCHEMAS = setOf("xt", "information_schema", "pg_catalog")

private fun checkNotForbidden(schema: SchemaName, table: TableName) {
    if (schema in FORBIDDEN_SCHEMAS) {
        throw Incorrect(
            "Cannot write to table: $schema.$table",
            "xtdb/forbidden-table",
            mapOf("schema" to schema, "table" to table)
        )
    }
}

private fun interface TxOpWriter<O : TxOp> {
    fun writeOp(op: O)
}

private class SqlWriter(val al: BufferAllocator, ops: VectorWriter) : TxOpWriter<TxOp.Sql> {
    private val sqlVec = ops.vectorFor("sql", structOf().fieldType)
    private val queryVec = sqlVec.vectorFor("query", UTF8.fieldType)
    private val argsVec = sqlVec.vectorFor("args", VAR_BINARY.fieldType)

    override fun writeOp(op: TxOp.Sql) {
        queryVec.writeObject(op.sql)

        op.args?.let { args ->
            args.openDirectSlice(al).use { argsVec.writeObject(it.asArrowStream) }
        }

        sqlVec.endStruct()
    }
}

private class PutDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.PutDocs> {
    private val putVec = ops.vectorFor("put-docs", structOf().fieldType)
    private val iidsVec = putVec.vectorFor("iids", listTypeOf(IID).fieldType)
    private val iidWriter = iidsVec.getListElements(IID.fieldType)
    private val docsVec = putVec.vectorFor("documents", unionOf().fieldType)
    private val validFromVec = putVec.vectorFor("_valid_from", maybe(INSTANT).fieldType)
    private val validToVec = putVec.vectorFor("_valid_to", maybe(INSTANT).fieldType)
    private val tableDocWriters = mutableMapOf<String, VectorWriter>()

    override fun writeOp(op: TxOp.PutDocs) {
        checkNotForbidden(op.schema, op.table)

        val schemaAndTable = "${op.schema}/${op.table}"
        val tableDocsWriter = tableDocWriters.getOrPut(schemaAndTable) {
            docsVec.vectorFor(schemaAndTable, just(LIST_TYPE).fieldType)
        }

        val tableDocWriter = tableDocsWriter.getListElements(just(STRUCT_TYPE).fieldType)

        val idVec = op.docs["_id"]
        for (idx in 0 until op.docs.rowCount) {
            val eid = idVec.getObject(idx)
            iidWriter.writeBytes(eid.asIid)
        }
        iidsVec.endList()

        tableDocWriter.append(RelationAsStructReader("docs", op.docs))
        tableDocsWriter.endList()

        op.validFrom?.let { validFromVec.writeObject(it) } ?: validFromVec.writeNull()
        op.validTo?.let { validToVec.writeObject(it) } ?: validToVec.writeNull()

        putVec.endStruct()
    }
}

private class PatchDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.PatchDocs> {
    private val patchVec = ops.vectorFor("patch-docs", structOf().fieldType)
    private val iidsVec = patchVec.vectorFor("iids", listTypeOf(IID).fieldType)
    private val iidWriter = iidsVec.getListElements(IID.fieldType)
    private val docsVec = patchVec.vectorFor("documents", unionOf().fieldType)
    private val validFromVec = patchVec.vectorFor("_valid_from", maybe(INSTANT).fieldType)
    private val validToVec = patchVec.vectorFor("_valid_to", maybe(INSTANT).fieldType)
    private val tableDocWriters = mutableMapOf<String, VectorWriter>()

    override fun writeOp(op: TxOp.PatchDocs) {
        checkNotForbidden(op.schema, op.table)

        val schemaAndTable = "${op.schema}/${op.table}"
        val tableDocWriter = tableDocWriters.getOrPut(schemaAndTable) {
            docsVec.vectorFor(schemaAndTable, listTypeOf(structOf()).fieldType)
                .also { it.getListElements(structOf().fieldType) }
        }

        val idVec = op.patches["_id"]
        for (idx in 0 until op.patches.rowCount) {
            val eid = idVec.getObject(idx)
            iidWriter.writeBytes(eid.asIid)
        }
        iidsVec.endList()

        tableDocWriter.writeObject(op.patches)

        op.validFrom?.let { validFromVec.writeObject(it) } ?: validFromVec.writeNull()
        op.validTo?.let { validToVec.writeObject(it) } ?: validToVec.writeNull()

        patchVec.endStruct()
    }
}

private class DeleteDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.DeleteDocs> {
    private val deleteVec = ops.vectorFor("delete-docs", structOf().fieldType)
    private val tableVec = deleteVec.vectorFor("table", UTF8.fieldType)
    private val iidsVec = deleteVec.vectorFor("iids", listTypeOf(IID).fieldType)
    private val iidWriter = iidsVec.getListElements(IID.fieldType)
    private val validFromVec = deleteVec.vectorFor("_valid_from", maybe(INSTANT).fieldType)
    private val validToVec = deleteVec.vectorFor("_valid_to", maybe(INSTANT).fieldType)

    override fun writeOp(op: TxOp.DeleteDocs) {
        checkNotForbidden(op.schema, op.table)

        if (op.ids.valueCount == 0) return

        tableVec.writeObject("${op.schema}/${op.table}")

        for (idx in 0 until op.ids.valueCount) {
            val docId = op.ids.getObject(idx)
            iidWriter.writeBytes(docId.asIid)
        }
        iidsVec.endList()

        op.validFrom?.let { validFromVec.writeObject(it) } ?: validFromVec.writeNull()
        op.validTo?.let { validToVec.writeObject(it) } ?: validToVec.writeNull()

        deleteVec.endStruct()
    }
}

private class EraseDocsWriter(ops: VectorWriter) : TxOpWriter<TxOp.EraseDocs> {
    private val eraseVec = ops.vectorFor("erase-docs", structOf().fieldType)
    private val tableVec = eraseVec.vectorFor("table", UTF8.fieldType)
    private val iidsVec = eraseVec.vectorFor("iids", listTypeOf(IID).fieldType)
    private val iidWriter = iidsVec.getListElements(IID.fieldType)

    override fun writeOp(op: TxOp.EraseDocs) {
        checkNotForbidden(op.schema, op.table)

        if (op.ids.valueCount == 0) return

        tableVec.writeObject("${op.schema}/${op.table}")

        for (idx in 0 until op.ids.valueCount) {
            val docId = op.ids.getObject(idx)
            iidWriter.writeBytes(docId.asIid)
        }
        iidsVec.endList()

        eraseVec.endStruct()
    }
}

data class TxOpts(
    val defaultTz: ZoneId? = null, val systemTime: Instant? = null,
    val user: String? = null, val userMetadata: Map<*, *>? = null
) {
    fun withFallbackTz(defaultTz: ZoneId?) = if (this.defaultTz != null) this else copy(defaultTz = defaultTz)
}

@JvmName("serializeTxOps")
fun List<TxOp>.toBytes(al: BufferAllocator, opts: TxOpts): ByteArray =
    Relation(al, txSchema).use { rel ->
        val txOpsVec = rel["tx-ops"]
        val txOpVec = txOpsVec.listElements

        val sqlWriter by lazy { SqlWriter(al, txOpVec) }
        val putDocsWriter by lazy { PutDocsWriter(txOpVec) }
        val patchDocsWriter by lazy { PatchDocsWriter(txOpVec) }
        val deleteDocsWriter by lazy { DeleteDocsWriter(txOpVec) }
        val eraseDocsWriter by lazy { EraseDocsWriter(txOpVec) }

        for (op in this) {
            when (op) {
                is TxOp.PutDocs -> putDocsWriter.writeOp(op)
                is TxOp.PatchDocs -> patchDocsWriter.writeOp(op)
                is TxOp.DeleteDocs -> deleteDocsWriter.writeOp(op)
                is TxOp.EraseDocs -> eraseDocsWriter.writeOp(op)
                is TxOp.Sql -> sqlWriter.writeOp(op)
            }
        }

        txOpsVec.endList()

        val sysTimeVec = rel["system-time"]
        opts.systemTime?.let { sysTimeVec.writeObject(it) } ?: sysTimeVec.writeNull()

        val defaultTz = checkNotNull(opts.defaultTz) { "missing defaultTz" }
        rel["default-tz"].writeObject(defaultTz.id)

        val userVec = rel["user"]
        opts.user?.let { userVec.writeObject(it) } ?: userVec.writeNull()

        rel["user-metadata"].writeObject(opts.userMetadata)

        rel.endRow()

        rel.asArrowStream
    }