package xtdb.tx

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation

class SharedEmptyBufferTxEncodingTest {

    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    private fun encodeOps() =
        listOf(
            TxOp.Sql("INSERT INTO t RECORDS {_id: ?, v: ?}", Relation.openFromRows(al, listOf(mapOf("?_0" to "b", "?_1" to 42L)))),
            TxOp.Sql("DELETE FROM t WHERE _id = 'a'"),
            TxOp.PutDocs.openFromRows(al, "public", "t", listOf(mapOf("_id" to "c", "s" to mapOf("x" to "y"))), null, null),
        ).let { ops ->
            try {
                ops.toArrowBytes(al)
            } finally {
                ops.forEach { it.close() }
            }
        }

    @Test
    fun `tx ops encode the same whatever the writer index of Arrow's shared empty buffer`() {
        val expected = encodeOps()

        // `allocator.empty` is one ArrowBuf for the whole JVM: moving its writer index stands in for another thread doing so.
        val shared = al.empty
        shared.writerIndex(4)
        val actual = try {
            encodeOps()
        } finally {
            shared.readerIndex(0)
            shared.writerIndex(0)
        }

        assertEquals(expected, actual)
    }
}
