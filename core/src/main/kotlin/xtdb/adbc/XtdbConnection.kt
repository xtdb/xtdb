package xtdb.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.core.AdbcStatement.QueryResult
import org.apache.arrow.adbc.core.AdbcStatement.UpdateResult
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.IResultCursor

class XtdbConnection(private val node: Node) : AdbcConnection {
    override fun createStatement() = object : AdbcStatement {
        private var sql: String? = null

        override fun setSqlQuery(sql: String) {
            this.sql = sql
        }

        override fun executeQuery(): QueryResult {
            val sql = this.sql ?: error("SQL query not set")
            val cursor = node.openSqlQuery(sql)

            return QueryResult(-1, object : ArrowReader(node.allocator) {
                override fun readSchema() = Schema(cursor.resultFields)

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
            node.executeSqlTx(this.sql ?: error("SQL query not set"))
            return UpdateResult(-1)
        }

        override fun prepare() = TODO("Not yet implemented")

        override fun close() = Unit
    }

    override fun getInfo(infoCodes: IntArray?) = TODO("Not yet implemented")

    interface Node {
        val allocator: BufferAllocator
        fun executeSqlTx(sql: String)
        fun openSqlQuery(sql: String): IResultCursor
    }

    override fun close() = Unit
}