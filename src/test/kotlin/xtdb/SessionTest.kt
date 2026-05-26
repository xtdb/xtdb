package xtdb

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import xtdb.api.log.MessageId
import xtdb.database.DatabaseName
import xtdb.database.decodeTxBasisToken
import xtdb.indexer.Snapshot
import xtdb.query.PreparedQuery
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import java.time.Instant

class SessionTest {

    private class FakeNode : Session.Node {
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

        override fun openSnapshot(dbName: DatabaseName, awaitToken: String?): Snapshot {
            snapshotTokens += (dbName to awaitToken); return mockk()
        }
    }

    private fun tokenOf(awaitToken: String?) = awaitToken?.decodeTxBasisToken()

    @Test
    fun `a submitted write is awaited by the next read`() {
        val node = FakeNode().apply { nextTxId = 7 }
        val session = Session(node, "mydb")

        session.submitTx(emptyList())
        session.openSqlQuery("SELECT 1")
        session.openSnapshot()

        val expectedToken = mapOf("mydb" to listOf(7L))
        assertEquals(expectedToken, tokenOf(node.queryTokens.single().second))
        assertEquals(expectedToken, tokenOf(node.snapshotTokens.single().second))
    }

    @Test
    fun `executeTx folds its tx into the token the same way as submitTx`() {
        val node = FakeNode().apply { nextTxId = 11 }
        val session = Session(node, "mydb")

        session.executeTx(emptyList())

        assertEquals(mapOf("mydb" to listOf(11L)), tokenOf(session.awaitToken))
    }

    @Test
    fun `successive writes accumulate monotonically`() {
        val node = FakeNode()
        val session = Session(node, "mydb")

        node.nextTxId = 4; session.submitTx(emptyList())
        node.nextTxId = 9; session.submitTx(emptyList())

        assertEquals(mapOf("mydb" to listOf(9L)), tokenOf(session.awaitToken))
    }

    @Test
    fun `retargeting dbName moves both writes and token onto the new db`() {
        val node = FakeNode().apply { nextTxId = 3 }
        val session = Session(node, "xtdb")

        session.dbName = "other"
        session.submitTx(emptyList())
        session.openSqlQuery("SELECT 1")

        assertEquals(listOf("other"), node.submittedDbs)
        assertEquals("other", node.queryTokens.single().first)
        assertEquals(mapOf("other" to listOf(3L)), tokenOf(session.awaitToken))
    }

    @Test
    fun `recordTx merges a foreign db without disturbing the session db`() {
        val node = FakeNode().apply { nextTxId = 2 }
        val session = Session(node, "secondary")

        session.submitTx(emptyList())
        session.recordTx("xtdb", 5)

        assertEquals(mapOf("secondary" to listOf(2L), "xtdb" to listOf(5L)), tokenOf(session.awaitToken))
    }

}
