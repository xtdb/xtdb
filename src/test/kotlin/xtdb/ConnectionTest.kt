package xtdb

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.Xtdb
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.database.Database
import xtdb.database.DatabaseName
import xtdb.database.decodeTxBasisToken
import xtdb.database.encodeTxBasisToken
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class ConnectionTest {

    private var nextTxId: MessageId = 0

    private lateinit var db: Database
    private lateinit var dbCat: Database.Catalog

    // components mocked only to feed return values — submitTxBlocking yields the next tx id and
    // awaitTxBlocking returns it committed; the assertions observe the connection's real await-token.
    private fun connection(dbName: DatabaseName): Xtdb.Connection {
        db = mockk()
        every { db.submitTxBlocking(any(), any()) } answers { Xtdb.SubmittedTx(nextTxId) }
        every { db.awaitTxBlocking(any(), any()) } answers {
            TransactionResult.Committed(mockk<TransactionKey> {
                every { txId } returns nextTxId
                every { systemTime } returns Instant.EPOCH
            })
        }

        dbCat = mockk(relaxed = true)
        every { dbCat.databaseOrNull(any()) } returns db
        every { dbCat.primary } returns db

        return Xtdb.Connection(mockk(relaxed = true), dbCat, mockk(relaxed = true), mockk(relaxed = true), Clock.systemUTC(), ZoneOffset.UTC, null, null, null, null, dbName)
    }

    private fun tokenOf(awaitToken: String?) = awaitToken?.decodeTxBasisToken()

    @Test
    fun `a write advances the connection's await-token`() {
        nextTxId = 7
        val conn = connection("mydb")

        conn.submitTx(emptyList())

        assertEquals(mapOf("mydb" to listOf(7L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `executeTx records its tx like submitTx`() {
        nextTxId = 11
        val conn = connection("mydb")

        conn.executeTx(emptyList())

        assertEquals(mapOf("mydb" to listOf(11L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `successive writes accumulate monotonically`() {
        val conn = connection("mydb")

        nextTxId = 4; conn.submitTx(emptyList())
        nextTxId = 9; conn.submitTx(emptyList())

        assertEquals(mapOf("mydb" to listOf(9L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `retargeting dbName moves writes and token onto the new db`() {
        nextTxId = 3
        val conn = connection("xtdb")

        conn.dbName = "other"
        conn.submitTx(emptyList())

        assertEquals(mapOf("other" to listOf(3L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `attachDb merges the primary db without disturbing the connection db`() {
        nextTxId = 2
        val conn = connection("secondary")
        conn.submitTx(emptyList())

        nextTxId = 5
        every { db.name } returns "xtdb"
        every { db.sendAttachDbMessage(any(), any()) } returns mockk<Log.MessageMetadata> {
            every { msgId } returns nextTxId
        }

        conn.attachDb("attached", Database.Config())

        assertEquals(mapOf("secondary" to listOf(2L), "xtdb" to listOf(5L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `the await token can be replaced directly - for SET AWAIT_TOKEN`() {
        nextTxId = 8
        val conn = connection("mydb")

        conn.submitTx(emptyList())
        conn.setAwaitToken(mapOf("mydb" to listOf(42L)).encodeTxBasisToken())

        assertEquals(mapOf("mydb" to listOf(42L)), tokenOf(conn.awaitToken))
    }

    @Test
    fun `executeTx populates the full last-submitted-tx record`() {
        nextTxId = 13
        val conn = connection("mydb")

        conn.executeTx(emptyList())

        val last = conn.lastSubmittedTx!!
        assertEquals(13L, last.txId)
        assertEquals(Instant.EPOCH, last.systemTime)
        assertEquals(true, last.committed)
        assertNull(last.error)
    }

    @Test
    fun `submitTx records only the tx-id in last-submitted-tx`() {
        nextTxId = 17
        val conn = connection("mydb")

        conn.submitTx(emptyList())

        val last = conn.lastSubmittedTx!!
        assertEquals(17L, last.txId)
        assertNull(last.systemTime)
        assertNull(last.committed)
        assertNull(last.error)
    }
}
