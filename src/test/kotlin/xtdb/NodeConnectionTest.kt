package xtdb

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.log.MessageId
import xtdb.database.DatabaseName
import xtdb.database.decodeTxBasisToken
import xtdb.database.encodeTxBasisToken
import xtdb.indexer.DatabaseSnapshot
import xtdb.query.PreparedQuery
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import java.time.Instant

class NodeConnectionTest {

    private class FakeNode : NodeConnection.Node {
        var nextTxId: MessageId = 0
        val submittedDbs = mutableListOf<DatabaseName>()
        val queryTokens = mutableListOf<Pair<DatabaseName, String?>>()
        val snapshotTokens = mutableListOf<Pair<DatabaseName, String?>>()

        override fun submitTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts) =
            Xtdb.SubmittedTx(nextTxId).also { submittedDbs += dbName }

        override fun executeTx(dbName: DatabaseName, ops: List<TxOp>, opts: TxOpts) =
            Xtdb.ExecutedTx(nextTxId, Instant.EPOCH, true, null).also { submittedDbs += dbName }

        override fun openSqlQuery(sql: String, dbName: DatabaseName, awaitToken: String?): ResultCursor {
            queryTokens += (dbName to awaitToken); return mockk()
        }

        override fun prepareSql(sql: String, dbName: DatabaseName, awaitToken: String?): PreparedQuery {
            queryTokens += (dbName to awaitToken); return mockk()
        }

        override fun openSnapshot(dbName: DatabaseName, awaitToken: String?): DatabaseSnapshot {
            snapshotTokens += (dbName to awaitToken); return mockk()
        }
    }

    private fun tokenOf(awaitToken: String?) = awaitToken?.decodeTxBasisToken()

    @Test
    fun `a write is awaited by the next read on the same connection`() {
        val node = FakeNode().apply { nextTxId = 7 }
        val conn = NodeConnection(node, "mydb")

        conn.submitTx(emptyList())
        conn.openSqlQuery("SELECT 1")
        conn.openSnapshot()

        val expected = mapOf("mydb" to listOf(7L))
        assertEquals(expected, tokenOf(node.queryTokens.single().second))
        assertEquals(expected, tokenOf(node.snapshotTokens.single().second))
    }

    @Test
    fun `executeTx records its tx like submitTx`() {
        val node = FakeNode().apply { nextTxId = 11 }
        val conn = NodeConnection(node, "mydb")

        conn.executeTx(emptyList())

        assertEquals(mapOf("mydb" to listOf(11L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `successive writes accumulate monotonically`() {
        val node = FakeNode()
        val conn = NodeConnection(node, "mydb")

        node.nextTxId = 4; conn.submitTx(emptyList())
        node.nextTxId = 9; conn.submitTx(emptyList())

        assertEquals(mapOf("mydb" to listOf(9L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `retargeting dbName moves both writes and token onto the new db`() {
        val node = FakeNode().apply { nextTxId = 3 }
        val conn = NodeConnection(node, "xtdb")

        conn.dbName = "other"
        conn.submitTx(emptyList())
        conn.openSqlQuery("SELECT 1")

        assertEquals(listOf("other"), node.submittedDbs)
        assertEquals("other", node.queryTokens.single().first)
        assertEquals(mapOf("other" to listOf(3L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `recordTx merges a foreign db without disturbing the connection db - the attach-detach case`() {
        val node = FakeNode().apply { nextTxId = 2 }
        val conn = NodeConnection(node, "secondary")

        conn.submitTx(emptyList())
        conn.recordTx("xtdb", 5)

        assertEquals(mapOf("secondary" to listOf(2L), "xtdb" to listOf(5L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `the await token can be replaced directly - for SET AWAIT_TOKEN`() {
        val node = FakeNode().apply { nextTxId = 8 }
        val conn = NodeConnection(node, "mydb")

        conn.submitTx(emptyList())
        conn.awaitToken = mapOf("mydb" to listOf(42L)).encodeTxBasisToken()

        assertEquals(mapOf("mydb" to listOf(42L)), tokenOf(conn.awaitToken))
    }
}
