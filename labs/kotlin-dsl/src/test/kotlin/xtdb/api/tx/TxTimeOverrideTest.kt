package xtdb.api.tx

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.aDocument
import utils.assert
import utils.putAndWait
import xtdb.api.TransactionInstant
import xtdb.api.XtdbDocument
import xtdb.api.XtdbKt
import xtdb.api.query.domain.XtdbDocumentSerde
import xtdb.api.tx.Transaction.buildTx
import xtdb.api.tx.TransactionContext.Companion.build
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.util.*
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TxTimeOverrideTest {
    private val xtdb = XtdbKt.startNode()

    private fun TransactionInstant.await() = xtdb.awaitTx(this, Duration.ofSeconds(10))

    @AfterAll
    fun afterAll() {
        xtdb.close()
    }

    @Nested
    inner class OverrideTxTime {
        private val String.date get() = Date.from(ZonedDateTime.parse(this).toInstant())

        @Test
        internal fun `can override txTime`(): Unit = xtdb.run {
            val doc1 = aDocument()
            val doc2 = aDocument()
            val doc3 = aDocument()
            val doc4 = aDocument()

            val tx1 = submitTx {
                put(doc1)
                setTxTime("2020-01-01T00:00:00Z".date)
            }.await()

            assertTrue(hasTxCommitted(tx1))

            assert {
                +doc1
            }

            val tx2 = submitTx {
                put(doc2)
                setTxTime("2019-01-01T00:00:00Z".date)
            }.also { it.await() }

            assertFalse(this.hasTxCommitted(tx2))

            val tx3 = submitTx {
                put(doc3)
                setTxTime("3000-01-01T00:00:00Z".date)
            }.also { it.await() }

            assertFalse(this.hasTxCommitted(tx3))

            val tx4 = submitTx {
                put(doc4)
            }.also { it.await() }

            assertTrue(this.hasTxCommitted(tx4))
        }
    }
}