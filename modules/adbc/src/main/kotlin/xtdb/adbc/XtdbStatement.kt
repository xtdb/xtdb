package xtdb.adbc

import clojure.lang.ILookup
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.IResultCursor
import xtdb.api.Xtdb
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.database.Database
import xtdb.query.IQuerySource
import xtdb.query.PreparedQuery

class XtdbStatement(
    private val allocator: BufferAllocator,
    private val node: Xtdb
) : AdbcStatement {

    private var sqlQuery: String? = null
    private var preparedQuery: PreparedQuery? = null
    private var nodeAllocator: BufferAllocator? = null
    private var boundParams: RelationReader? = null

    override fun setSqlQuery(query: String?) {
        this.sqlQuery = query
        this.preparedQuery = null
    }

    override fun prepare() {
        val sql = sqlQuery ?: throw IllegalStateException("No SQL query set")

        val nodeMap = node as? ILookup ?: throw IllegalStateException("Node does not support field access")

        nodeAllocator = nodeMap.valAt(clojure.lang.Keyword.intern("allocator")) as? BufferAllocator
            ?: throw IllegalStateException("Cannot access allocator from node")

        val qSrc = nodeMap.valAt(clojure.lang.Keyword.intern("q-src")) as? IQuerySource
            ?: throw IllegalStateException("Cannot access query source from node")

        val dbCat = nodeMap.valAt(clojure.lang.Keyword.intern("db-cat")) as? Database.Catalog
            ?: throw IllegalStateException("Cannot access database catalog from node")

        val opts = clojure.lang.PersistentHashMap.create(
            clojure.lang.Keyword.intern("default-db"), "xtdb"
        )

        preparedQuery = qSrc.prepareQuery(sql, dbCat, opts)
    }

    override fun bind(root: VectorSchemaRoot?) {
        if (root == null) {
            boundParams = null
            return
        }

        val alloc = nodeAllocator ?: throw IllegalStateException("Statement not prepared")

        // Convert VectorSchemaRoot to a Relation for parameter binding
        // Note: Relation.fromRoot shares buffers with the root, so we shouldn't close it
        val paramRel = Relation.fromRoot(alloc, root)
        boundParams = paramRel
    }

    override fun executeQuery(): AdbcStatement.QueryResult {
        val alloc = nodeAllocator ?: run {
            // If not prepared, prepare now
            prepare()
            nodeAllocator!!
        }

        val pq = preparedQuery ?: run {
            // If not prepared, do it now
            prepare()
            preparedQuery!!
        }

        // Build options map with parameters if bound
        val opts = if (boundParams != null) {
            clojure.lang.PersistentHashMap.create(
                clojure.lang.Keyword.intern("args"), boundParams
            )
        } else {
            emptyMap<Any, Any>()
        }

        val cursor: IResultCursor = pq.openQuery(opts)
        val reader = XtdbArrowReader(alloc, cursor)

        return AdbcStatement.QueryResult(-1, reader)
    }

    override fun executeUpdate(): AdbcStatement.UpdateResult {
        executeQuery().use { result ->
            return AdbcStatement.UpdateResult(-1)
        }
    }

    override fun close() {
        // Don't close boundParams - it shares buffers with the caller's VectorSchemaRoot
        // which will be closed by the caller
        preparedQuery = null
        boundParams = null
    }

    private class XtdbArrowReader(
        allocator: BufferAllocator,
        private val cursor: IResultCursor
    ) : ArrowReader(allocator) {

        private val vsr: VectorSchemaRoot = VectorSchemaRoot.create(
            Schema(cursor.resultFields),
            allocator
        )
        private val loader = VectorLoader(vsr)

        override fun loadNextBatch(): Boolean {
            // Clear vectors from previous batch to prepare for new data
            vsr.fieldVectors.forEach { it.clear() }

            val loaded = booleanArrayOf(false)

            cursor.tryAdvance { rel ->
                // Open direct slice with our allocator (shares memory with node)
                val directSlice = rel.openDirectSlice(allocator)
                directSlice.use {
                    val recordBatch = directSlice.openArrowRecordBatch()
                    recordBatch.use {
                        // Load batch into VectorSchemaRoot
                        // This is still a copy, but it's the minimum required
                        // for ADBC's API which expects VectorSchemaRoot
                        loader.load(recordBatch)
                        loaded[0] = true
                    }
                }
            }

            return loaded[0]
        }

        override fun closeReadSource() {
            try {
                vsr.close()
            } finally {
                cursor.close()
            }
        }

        override fun getVectorSchemaRoot(): VectorSchemaRoot = vsr

        override fun bytesRead(): Long = 0

        override fun readSchema(): Schema = vsr.schema
    }
}
