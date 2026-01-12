package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.core.AdbcStatement.QueryResult
import org.apache.arrow.adbc.core.AdbcStatement.UpdateResult
import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.IResultCursor
import xtdb.api.Xtdb
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.unsupported
import xtdb.database.DatabaseName
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import xtdb.util.useAll

class XtdbConnection(private val node: Node) : AdbcConnection {

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

            return QueryResult(-1, object : ArrowReader(node.allocator) {
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
            })
        }

        override fun executeUpdate(): UpdateResult {
            listOf(TxOp.Sql(sql ?: error("SQL query not set"))).useAll { ops ->
                // TODO multi-db
                node.executeTx("xtdb", ops)
            }
            return UpdateResult(-1)
        }

        override fun prepare() = TODO("Not yet implemented")


        override fun close() = Unit
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

    override fun getInfo(infoCodes: IntArray?) = TODO("Not yet implemented")

    interface Node {
        val allocator: BufferAllocator
        fun submitTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.SubmittedTx
        fun executeTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts = TxOpts()): Xtdb.ExecutedTx
        fun openSqlQuery(sql: String): IResultCursor
    }

    override fun close() = Unit
}