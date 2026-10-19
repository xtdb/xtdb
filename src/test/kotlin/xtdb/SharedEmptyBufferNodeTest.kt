package xtdb

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorUnloader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation

class SharedEmptyBufferNodeTest {

    private fun Xtdb.Connection.streamRows(al: BufferAllocator, sql: String) =
        createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.executeQuery().reader.use { rdr ->
                buildList {
                    while (rdr.loadNextBatch()) {
                        // what the Flight SQL server does with each buffer of a batch it streams
                        VectorUnloader(rdr.vectorSchemaRoot).recordBatch.use { rb -> rb.buffers.forEach { it.nioBuffer() } }
                        Relation.fromRoot(al, rdr.vectorSchemaRoot).use { addAll(it.toMaps(SNAKE_CASE_STRING)) }
                    }
                }
            }
        }

    @Test
    fun `writes and Arrow result streams don't depend on Arrow's shared empty buffer`() {
        Xtdb.openNode().use { node ->
            RootAllocator().use { al ->
                // `allocator.empty` is one ArrowBuf for the whole JVM: moving its writer index stands in for another thread doing so.
                val shared = al.empty
                shared.writerIndex(777)
                try {
                    node.connect().use { conn ->
                        conn.createStatement().use { stmt ->
                            stmt.setSqlQuery("INSERT INTO docs RECORDS {_id: 1, s: {a: 'x'}, l: [1, 2], v: 'str', m: [{c: 'x'}]}, {_id: 2, v: 3, m: 'flat'}")
                            stmt.executeUpdate()
                        }

                        val row1 = mapOf("_id" to 1L, "s" to mapOf("a" to "x"), "l" to listOf(1L, 2L), "v" to "str", "m" to listOf(mapOf("c" to "x")))
                        val row2 = mapOf("_id" to 2L, "v" to 3L, "m" to "flat")

                        assertEquals(listOf(row1, row2), conn.streamRows(al, "SELECT _id, s, l, v, m FROM docs ORDER BY _id"))
                    }
                } finally {
                    shared.readerIndex(0)
                    shared.writerIndex(0)
                }
            }
        }
    }
}
