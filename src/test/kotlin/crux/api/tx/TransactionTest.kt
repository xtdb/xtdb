package crux.api.tx

import crux.api.CruxK
import crux.api.TransactionInstant
import crux.api.tx.Transaction.*
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.aDocument
import utils.assertDocument
import utils.assertNoDocument
import utils.putAndWait
import java.time.Duration
import java.time.Instant
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionTest {
    private val crux = CruxK.startNode()

    private fun TransactionInstant.await() = crux.awaitTx(this, Duration.ofSeconds(10))

    // Provides 10 distinct dates in ascending order
    private val dates = List(10) { Date.from(Instant.now().plusSeconds(it.toLong())) }

    @Nested
    inner class BaseJavaCrux {
        /**
         * This is for testing that all is well with java and providing a comparison with our new DSL
         */

        @Test
        fun `can put a document`() {
            val document = aDocument()
            crux.submitTx(
                buildTx {
                    it.put(document)
                }
            ).await()
            crux.assertDocument(document)
        }

        @Test
        fun `can put a document at time`() {
            val document = aDocument()
            crux.submitTx(
                buildTx {
                    it.put(document, dates[1])
                }
            ).await()
            crux.assertNoDocument(document.id, dates[0])
            crux.assertDocument(document, dates[1])
        }

        @Test
        fun `can put a document with end valid time`() {
            val document = aDocument()

            crux.submitTx(
                buildTx {
                    it.put(document, dates[1], dates[2])
                }
            ).await()

            crux.assertNoDocument(document.id, dates[0])
            crux.assertDocument(document, dates[1])
            crux.assertNoDocument(document.id, dates[2])
        }

        @Test
        fun `can delete a document`() {
            val document = aDocument()
            crux.putAndWait(document)

            crux.submitTx(
                buildTx {
                    it.delete(document.id)
                }
            ).await()

            crux.assertNoDocument(document.id)
        }

        @Test
        fun `can delete a document at valid time`() {
            val document = aDocument()
            crux.putAndWait(document, dates[0])

            crux.submitTx (
                buildTx {
                    it.delete(document.id, dates[1])
                }
            ).await()

            crux.assertDocument(document, dates[0])
            crux.assertNoDocument(document.id, dates[1])
        }

        @Test
        fun `can delete a document with end valid time`() {
            val document = aDocument()
            crux.putAndWait(document, dates[0])

            crux.submitTx (
                buildTx {
                    it.delete(document.id, dates[1], dates[2])
                }
            ).await()

            crux.assertDocument(document, dates[0])
            crux.assertNoDocument(document.id, dates[1])
            crux.assertDocument(document, dates[2])
        }

        @Test
        fun `can evict a document`() {
            val document = aDocument()
            crux.putAndWait(document)

            crux.submitTx (
                buildTx {
                    it.evict(document.id)
                }
            ).await()

            crux.assertNoDocument(document.id)
        }

        @Test
        fun `can match a document`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.submitTx (
                buildTx {
                    it.match(matchDoc)
                    it.put(document)
                }
            ).await()

            crux.assertNoDocument(document.id)

            crux.putAndWait(matchDoc)

            crux.submitTx (
                buildTx {
                    it.match(matchDoc)
                    it.put(document)
                }
            ).await()

            crux.assertDocument(document)
        }

        @Test
        fun `can match a document at a valid time`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.putAndWait(matchDoc, dates[1])

            crux.submitTx (
                buildTx {
                    it.match(matchDoc, dates[0])
                    it.put(document)
                }
            ).await()

            crux.assertNoDocument(document.id)

            crux.submitTx (
                buildTx {
                    it.match(matchDoc, dates[1])
                    it.put(document)
                }
            ).await()

            crux.assertDocument(document)
        }

        @Test
        fun `can match against no document`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.putAndWait(matchDoc)

            crux.submitTx (
                buildTx {
                    it.matchNotExists(matchDoc.id)
                    it.put(document)
                }
            ).await()

            crux.assertNoDocument(document.id)

            crux.submitTx (
                buildTx {
                    it.matchNotExists(UUID.randomUUID().toString())
                    it.put(document)
                }
            ).await()

            crux.assertDocument(document)
        }

        @Test
        fun `can match against a no document at valid time`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.putAndWait(matchDoc, dates[1])

            crux.submitTx(
                buildTx {
                    it.matchNotExists(matchDoc.id, dates[1])
                    it.put(document)
                }
            ).await()

            crux.assertNoDocument(document.id)

            crux.submitTx(
                buildTx {
                    it.matchNotExists(matchDoc.id, dates[0])
                    it.put(document)
                }
            ).await()

            crux.assertDocument(document)
        }
    }

    @Nested
    inner class KotlinDSL {
        @Test
        fun `can put a document`() {
            val document = aDocument()

            crux.submitTx {
                + document
            }.await()

            crux.assertDocument(document)
        }

        @Test
        fun `can put a document at time`() {
            val document = aDocument()
            crux.submitTx {
                + document from dates[1]
            }.await()
            crux.assertNoDocument(document.id, dates[0])
            crux.assertDocument(document, dates[1])
        }

        @Test
        fun `can put a document with end valid time`() {
            val document = aDocument()

            crux.submitTx{
                + document from dates[1] to dates[2]
            }.await()

            crux.assertNoDocument(document.id, dates[0])
            crux.assertDocument(document, dates[1])
            crux.assertNoDocument(document.id, dates[2])
        }

        @Test
        fun `can delete a document`() {
            val document = aDocument()
            crux.putAndWait(document)

            crux.submitTx {
                - document.id
            }.await()

            crux.assertNoDocument(document.id)
        }

        @Test
        fun `can delete a document at valid time`() {
            val document = aDocument()
            crux.putAndWait(document, dates[0])

            crux.submitTx {
                - document.id from dates[1]
            }.await()

            crux.assertDocument(document, dates[0])
            crux.assertNoDocument(document.id, dates[1])
        }

        @Test
        fun `can delete a document with end valid time`() {
            val document = aDocument()
            crux.putAndWait(document, dates[0])

            crux.submitTx {
                - document.id from dates[1] to dates[2]
            }.await()

            crux.assertDocument(document, dates[0])
            crux.assertNoDocument(document.id, dates[1])
            crux.assertDocument(document, dates[2])
        }

        @Test
        fun `can evict a document`() {
            val document = aDocument()
            crux.putAndWait(document)

            crux.submitTx {
                evict(document.id)
            }.await()

            crux.assertNoDocument(document.id)
        }

        @Test
        fun `can match a document`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.submitTx {
                match(matchDoc)
                + document
            }.await()

            crux.assertNoDocument(document.id)

            crux.putAndWait(matchDoc)

            crux.submitTx {
                match(matchDoc)
                + document
            }.await()

            crux.assertDocument(document)
        }

        @Test
        fun `can match a document at a valid time`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.putAndWait(matchDoc, dates[1])

            crux.submitTx {
                match(matchDoc) at dates[0]
                + document
            }.await()

            crux.assertNoDocument(document.id)

            crux.submitTx {
                match(matchDoc) at dates[1]
                + document
            }.await()

            crux.assertDocument(document)
        }

        @Test
        fun `can match against no document`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.putAndWait(matchDoc)

            crux.submitTx {
                notExists(matchDoc.id)
                + document
            }.await()

            crux.assertNoDocument(document.id)

            crux.submitTx {
                notExists(UUID.randomUUID().toString())
                + document
            }.await()

            crux.assertDocument(document)
        }

        @Test
        fun `can match against a no document at valid time`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.putAndWait(matchDoc, dates[1])

            crux.submitTx {
                notExists(matchDoc.id) at dates[1]
                + document
            }.await()

            crux.assertNoDocument(document.id)

            crux.submitTx {
                notExists(matchDoc.id) at dates[0]
                + document
            }.await()

            crux.assertDocument(document)
        }
    }
}