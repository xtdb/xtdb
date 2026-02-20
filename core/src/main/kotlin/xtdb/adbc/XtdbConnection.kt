package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth
import org.apache.arrow.adbc.core.AdbcInfoCode
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.core.AdbcStatement.QueryResult
import org.apache.arrow.adbc.core.AdbcStatement.UpdateResult
import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.adbc.core.StandardSchemas
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.VectorUnloader
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.writer.VarCharWriter
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.IResultCursor
import xtdb.api.Xtdb
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import xtdb.arrow.unsupported
import xtdb.database.DatabaseName
import xtdb.table.TableRef
import xtdb.query.PreparedQuery
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import xtdb.util.useAll

class XtdbConnection(private val node: Node) : AdbcConnection {

    private var autoCommit = true
    private val pendingOps = mutableListOf<TxOp>()

    interface XtdbStatement : AdbcStatement {
        fun bind(rel: RelationReader): Unit = unsupported("bind(RelationReader) not supported")
    }

    override fun createStatement() = object : XtdbStatement {
        private var sql: String? = null

        override fun setSqlQuery(sql: String) {
            this.sql = sql
        }

        override fun executeQuery(): QueryResult {
            val sql = this.sql ?: error("SQL query not set")
            val cursor = node.openSqlQuery(sql)
            val schema = Schema(cursor.resultTypes.map { (name, type) -> type.toField(name) })

            return QueryResult(-1, cursorToArrowReader(cursor, schema))
        }

        override fun executeUpdate(): UpdateResult {
            executeDml(TxOp.Sql(sql ?: error("SQL query not set")))
            return UpdateResult(-1)
        }

        override fun prepare() = TODO("Not yet implemented")

        override fun close() = Unit
    }

    internal fun executeDml(op: TxOp) {
        if (autoCommit) {
            listOf(op).useAll { ops ->
                // TODO multi-db
                node.executeTx("xtdb", ops)
            }
        } else {
            pendingOps.add(op)
        }
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        this.autoCommit = autoCommit
    }

    override fun commit() {
        val ops = pendingOps.toList()
        pendingOps.clear()
        if (ops.isNotEmpty()) {
            ops.useAll {
                // TODO multi-db
                node.executeTx("xtdb", ops)
            }
        }
    }

    override fun rollback() {
        pendingOps.forEach { it.close() }
        pendingOps.clear()
    }

    override fun bulkIngest(targetTableName: String, mode: BulkIngestMode): XtdbStatement {
        require(mode == BulkIngestMode.CREATE_APPEND) { "Only CREATE_APPEND mode is supported" }

        val splitTable = targetTableName.split('.').reversed()
        val tableName = splitTable.first()
        val schemaName = splitTable.getOrNull(1) ?: "public"

        return object : XtdbStatement {
            override fun executeQuery() = error("Bulk ingest does not support queries")
            override fun prepare() = error("Bulk ingest does not support prepare")

            private var rel: Relation? = null

            override fun close() {
                rel.also { rel = null }?.close()
            }

            override fun bind(root: VectorSchemaRoot) {
                this.rel?.close()
                rel = Relation.fromRoot(node.allocator, root)
            }

            override fun bind(rel: RelationReader) {
                this.rel = rel.openDirectSlice(node.allocator)
            }

            override fun executeUpdate(): UpdateResult {
                return (rel.also { this.rel = null } ?: return UpdateResult(0))
                    .use { rel ->
                        // TODO multi-db
                        // TODO valid-from/valid-to for the batch
                        node.executeTx("xtdb", listOf(TxOp.PutDocs(schemaName, tableName, docs = rel)))

                        close()

                        UpdateResult(rel.rowCount.toLong())
                    }
            }
        }
    }

    fun prepareSql(sql: String): PreparedQuery = node.prepareSql(sql)
    fun openSqlQuery(sql: String): IResultCursor = node.openSqlQuery(sql)

    private fun cursorToArrowReader(cursor: IResultCursor, schema: Schema): ArrowReader =
        object : ArrowReader(node.allocator) {
            override fun readSchema() = schema

            override fun loadNextBatch(): Boolean =
                cursor.tryAdvance { inRel ->
                    inRel.openDirectSlice(allocator).use { rel ->
                        val loader = VectorLoader(vectorSchemaRoot)
                        rel.openArrowRecordBatch().use { rb ->
                            loader.load(rb)
                        }
                    }
                }

            override fun bytesRead() = -1L

            override fun closeReadSource() = cursor.close()
        }

    /**
     * Returns an ArrowReader that yields a single batch, built from a pre-populated VectorSchemaRoot.
     */
    private fun singleBatchReader(schema: Schema, populate: (BufferAllocator, VectorSchemaRoot) -> Unit): ArrowReader =
        object : ArrowReader(node.allocator) {
            private var consumed = false

            override fun readSchema() = schema

            override fun loadNextBatch(): Boolean {
                if (consumed) return false
                consumed = true

                VectorSchemaRoot.create(schema, allocator).use { tmpRoot ->
                    tmpRoot.allocateNew()
                    populate(allocator, tmpRoot)

                    val loader = VectorLoader(vectorSchemaRoot)
                    VectorUnloader(tmpRoot).recordBatch.use { rb ->
                        loader.load(rb)
                    }
                }

                return true
            }

            override fun bytesRead() = -1L
            override fun closeReadSource() = Unit
        }

    override fun getTableTypes(): ArrowReader =
        singleBatchReader(StandardSchemas.TABLE_TYPES_SCHEMA) { _, root ->
            val vec = root.getVector("table_type") as VarCharVector
            vec.setSafe(0, "TABLE".toByteArray())
            root.rowCount = 1
        }

    override fun getInfo(infoCodes: IntArray?): ArrowReader {
        val codes = infoCodes?.toSet() ?: AdbcInfoCode.entries.map { it.value }.toSet()

        return singleBatchReader(StandardSchemas.GET_INFO_SCHEMA) { _, root ->
            val infoNameVec = root.getVector("info_name") as UInt4Vector
            val infoValueVec = root.getVector("info_value") as DenseUnionVector
            val stringVec = infoValueVec.getVarCharVector(0)

            var idx = 0
            fun addStringInfo(code: AdbcInfoCode, value: String) {
                if (code.value !in codes) return
                infoNameVec.setSafe(idx, code.value)
                infoValueVec.setTypeId(idx, 0)
                infoValueVec.offsetBuffer.setInt(idx.toLong() * DenseUnionVector.OFFSET_WIDTH, idx)
                stringVec.setSafe(idx, value.toByteArray())
                idx++
            }

            addStringInfo(AdbcInfoCode.VENDOR_NAME, "XTDB")
            addStringInfo(AdbcInfoCode.VENDOR_VERSION, "dev")
            addStringInfo(AdbcInfoCode.DRIVER_NAME, "XTDB ADBC Driver")
            addStringInfo(AdbcInfoCode.DRIVER_VERSION, "dev")

            infoValueVec.valueCount = idx
            root.rowCount = idx
        }
    }

    override fun getTableSchema(catalog: String?, dbSchema: String?, tableName: String): Schema {
        // TODO multi-db: use catalog to resolve dbName
        val table = TableRef("xtdb", dbSchema ?: "public", tableName)
        val types = node.getColumnTypes(table) ?: return Schema(emptyList())
        return Schema(types.entries.map { (name, type) -> type.toField(name) })
    }

    override fun getObjects(
        depth: GetObjectsDepth,
        catalogPattern: String?,
        dbSchemaPattern: String?,
        tableNamePattern: String?,
        tableTypes: Array<out String>?,
        columnNamePattern: String?
    ): ArrowReader {
        // Build the nested getObjects response by querying information_schema.
        // Currently reports a single "xtdb" catalog.

        return singleBatchReader(StandardSchemas.GET_OBJECTS_SCHEMA) { al, root ->
            val catalogNameVec = root.getVector("catalog_name") as VarCharVector
            val dbSchemasVec = root.getVector("catalog_db_schemas") as ListVector

            catalogNameVec.setSafe(0, "xtdb".toByteArray())

            if (depth == GetObjectsDepth.CATALOGS) {
                dbSchemasVec.setNull(0)
                root.rowCount = 1
                return@singleBatchReader
            }

            val writer = dbSchemasVec.writer
            writer.setPosition(0)
            writer.startList()

            val schemas = querySchemas(dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern, depth)

            for ((schemaName, tables) in schemas) {
                writer.struct().start()
                writeVarChar(al, writer.struct().varChar("db_schema_name"), schemaName)

                if (depth == GetObjectsDepth.DB_SCHEMAS) {
                    writer.struct().list("db_schema_tables").writeNull()
                } else {
                    val tableListWriter = writer.struct().list("db_schema_tables")
                    tableListWriter.startList()

                    for (table in tables) {
                        val tw = tableListWriter.struct()
                        tw.start()
                        writeVarChar(al, tw.varChar("table_name"), table.name)
                        writeVarChar(al, tw.varChar("table_type"), "TABLE")

                        if (depth == GetObjectsDepth.TABLES || table.columns.isEmpty()) {
                            tw.list("table_columns").writeNull()
                        } else {
                            val colListWriter = tw.list("table_columns")
                            colListWriter.startList()
                            for ((colIdx, col) in table.columns.withIndex()) {
                                val cw = colListWriter.struct()
                                cw.start()
                                writeVarChar(al, cw.varChar("column_name"), col.name)
                                cw.integer("ordinal_position").writeInt(colIdx + 1)
                                // remaining xdbc fields are null
                                cw.end()
                            }
                            colListWriter.endList()
                        }

                        tw.list("table_constraints").writeNull()
                        tw.end()
                    }

                    tableListWriter.endList()
                }

                writer.struct().end()
            }

            writer.endList()
            root.rowCount = 1
        }
    }

    private data class ColumnInfo(val name: String, val dataType: String)
    private data class TableInfo(val name: String, val columns: List<ColumnInfo>)

    private fun querySchemas(
        dbSchemaPattern: String?,
        tableNamePattern: String?,
        tableTypes: Array<out String>?,
        columnNamePattern: String?,
        depth: GetObjectsDepth
    ): Map<String, List<TableInfo>> {
        // XTDB only has table_type='TABLE' currently
        if (tableTypes != null && "TABLE" !in tableTypes) return emptyMap()

        val conditions = mutableListOf<String>()
        dbSchemaPattern?.let { conditions.add("table_schema LIKE '${it.replace("'", "''")}'") }
        tableNamePattern?.let { conditions.add("table_name LIKE '${it.replace("'", "''")}'") }

        val where = if (conditions.isNotEmpty()) "WHERE ${conditions.joinToString(" AND ")}" else ""

        if (depth == GetObjectsDepth.DB_SCHEMAS) {
            val sql = "SELECT DISTINCT table_schema FROM information_schema.tables $where ORDER BY table_schema"
            val result = linkedMapOf<String, List<TableInfo>>()
            node.openSqlQuery(sql).use { cursor ->
                cursor.forEachRemaining { rel ->
                    for (i in 0 until rel.rowCount) {
                        result[rel.get("table_schema").getObject(i).toString()] = emptyList()
                    }
                }
            }
            return result
        }

        val needColumns = depth == GetObjectsDepth.ALL
        val sql = if (needColumns) {
            val colConditions = conditions.toMutableList()
            columnNamePattern?.let { colConditions.add("column_name LIKE '${it.replace("'", "''")}'") }
            val colWhere = if (colConditions.isNotEmpty()) "WHERE ${colConditions.joinToString(" AND ")}" else ""
            "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns $colWhere ORDER BY table_schema, table_name, ordinal_position"
        } else {
            "SELECT DISTINCT table_schema, table_name FROM information_schema.tables $where ORDER BY table_schema, table_name"
        }

        val result = linkedMapOf<String, MutableList<TableInfo>>()

        if (needColumns) {
            val tableColumns = linkedMapOf<Pair<String, String>, MutableList<ColumnInfo>>()
            node.openSqlQuery(sql).use { cursor ->
                cursor.forEachRemaining { rel ->
                    for (i in 0 until rel.rowCount) {
                        val schema = rel.get("table_schema").getObject(i).toString()
                        val table = rel.get("table_name").getObject(i).toString()
                        val column = rel.get("column_name").getObject(i).toString()
                        val dataType = rel.get("data_type").getObject(i).toString()
                        tableColumns.getOrPut(schema to table) { mutableListOf() }
                            .add(ColumnInfo(column, dataType))
                    }
                }
            }
            for ((key, cols) in tableColumns) {
                result.getOrPut(key.first) { mutableListOf() }
                    .add(TableInfo(key.second, cols))
            }
        } else {
            node.openSqlQuery(sql).use { cursor ->
                cursor.forEachRemaining { rel ->
                    for (i in 0 until rel.rowCount) {
                        val schema = rel.get("table_schema").getObject(i).toString()
                        val table = rel.get("table_name").getObject(i).toString()
                        result.getOrPut(schema) { mutableListOf() }
                            .add(TableInfo(table, emptyList()))
                    }
                }
            }
        }

        return result
    }

    override fun getCurrentCatalog(): String = "xtdb"
    override fun getCurrentDbSchema(): String = "public"

    interface Node {
        val allocator: BufferAllocator
        fun submitTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.SubmittedTx
        fun executeTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.ExecutedTx
        fun openSqlQuery(sql: String): IResultCursor
        fun prepareSql(sql: String): PreparedQuery
        fun getColumnTypes(table: TableRef): Map<String, VectorType>?
    }

    override fun close() {
        rollback()
    }

    companion object {
        private fun writeVarChar(al: BufferAllocator, writer: VarCharWriter, s: String) {
            val bytes = s.toByteArray()
            al.buffer(bytes.size.toLong()).use { buf ->
                buf.writeBytes(bytes)
                writer.writeVarChar(0, bytes.size, buf)
            }
        }

    }
}
