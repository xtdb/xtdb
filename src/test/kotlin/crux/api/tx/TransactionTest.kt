package crux.api.tx

import crux.api.CruxK
import crux.api.TransactionInstant
import crux.api.tx.Transaction.*
import crux.api.tx.TransactionContext.Companion.build
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.aDocument
import utils.putAndWait
import utils.assert
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.test.assertEquals

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

            crux.assert {
                + document
            }
        }

        @Test
        fun `can put a document at time`() {
            val document = aDocument()

            crux.submitTx(
                buildTx {
                    it.put(document, dates[1])
                }
            ).await()

            crux.assert {
                document notAt dates[0]
                document at dates[1]
            }
        }

        @Test
        fun `can put a document with end valid time`() {
            val document = aDocument()

            crux.submitTx(
                buildTx {
                    it.put(document, dates[1], dates[2])
                }
            ).await()

            crux.assert {
                document notAt dates[0]
                document at dates[1]
                document notAt dates[2]
            }
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

            crux.assert {
                - document
            }
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

            crux.assert {
                document at dates[0]
                document notAt dates[1]
            }
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

            crux.assert {
                document at dates[0]
                document notAt dates[1]
                document at dates[2]
            }
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

            crux.assert {
                - document
            }
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

            crux.assert {
                - document
            }

            crux.putAndWait(matchDoc)

            crux.submitTx (
                buildTx {
                    it.match(matchDoc)
                    it.put(document)
                }
            ).await()

            crux.assert {
                + document
            }
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

            crux.assert {
                - document
            }

            crux.submitTx (
                buildTx {
                    it.match(matchDoc, dates[1])
                    it.put(document)
                }
            ).await()

            crux.assert {
                + document
            }
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

            crux.assert {
                - document
            }

            crux.submitTx (
                buildTx {
                    it.matchNotExists(UUID.randomUUID().toString())
                    it.put(document)
                }
            ).await()

            crux.assert {
                + document
            }
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

            crux.assert {
                - document
            }

            crux.submitTx(
                buildTx {
                    it.matchNotExists(matchDoc.id, dates[0])
                    it.put(document)
                }
            ).await()

            crux.assert {
                + document
            }
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

            crux.assert {
                + document
            }
        }

        @Test
        fun `can put a document at time`() {
            val document = aDocument()
            crux.submitTx {
                + document from dates[1]
            }.await()

            crux.assert {
                document notAt dates[0]
                document at dates[1]
            }
        }

        @Test
        fun `can put a document with end valid time`() {
            val document = aDocument()

            crux.submitTx{
                + document from dates[1] until dates[2]
            }.await()

            crux.assert {
                document notAt dates[0]
                document at dates[1]
                document notAt dates[2]
            }
        }

        @Test
        fun `can delete a document`() {
            val document = aDocument()
            crux.putAndWait(document)

            crux.submitTx {
                - document.id
            }.await()

            crux.assert {
                - document
            }
        }

        @Test
        fun `can delete a document at valid time`() {
            val document = aDocument()
            crux.putAndWait(document, dates[0])

            crux.submitTx {
                - document.id from dates[1]
            }.await()

            crux.assert {
                document at dates[0]
                document notAt dates[1]
            }
        }

        @Test
        fun `can delete a document with end valid time`() {
            val document = aDocument()
            crux.putAndWait(document, dates[0])

            crux.submitTx {
                -document.id from dates[1] until dates[2]
            }.await()

            crux.assert {
                document at dates[0]
                document notAt dates[1]
                document at dates[2]
            }
        }

        @Test
        fun `can evict a document`() {
            val document = aDocument()
            crux.putAndWait(document)

            crux.submitTx {
                evict(document.id)
            }.await()

            crux.assert {
                - document
            }
        }

        @Test
        fun `can match a document`() {
            val matchDoc = aDocument()
            val document = aDocument()

            crux.submitTx {
                match(matchDoc)
                + document
            }.await()

            crux.assert {
                - document
            }

            crux.putAndWait(matchDoc)

            crux.submitTx {
                match(matchDoc)
                + document
            }.await()

            crux.assert {
                + document
            }
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

            crux.assert {
                - document
            }

            crux.submitTx {
                match(matchDoc) at dates[1]
                + document
            }.await()

            crux.assert {
                + document
            }
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

            crux.assert {
                - document
            }

            crux.submitTx {
                notExists(UUID.randomUUID().toString())
                + document
            }.await()

            crux.assert {
                + document
            }
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

            crux.assert {
                - document
            }

            crux.submitTx {
                notExists(matchDoc.id) at dates[0]
                + document
            }.await()

            crux.assert {
                + document
            }
        }
    }

    @Nested
    inner class TransactionDslGroupings {
        @Test
        fun `valid time groupings`() {
            val document1 = aDocument()
            val document2 = aDocument()
            val document3 = aDocument()

            crux.submitTx {
                + document1 from dates[0]

                from(dates[1]) {
                    + document2
                    + document3 until dates[2]
                }
            }.await()

            crux.assert {
                at(dates[0]) {
                    + document1
                    - document2
                    - document3
                }

                at(dates[1]) {
                    + document2
                    + document3
                }

                at(dates[2]) {
                    + document2
                    - document3
                }
            }

            crux.submitTx {
                from(dates[3]) {
                    - document1.id
                    - document2.id until dates[4]
                }
            }.await()

            crux.assert {
                at(dates[2]) {
                    + document1
                    + document2
                }

                at(dates[3]) {
                    - document1
                    - document2
                }

                at(dates[4]) {
                    - document1
                    + document2
                }
            }
        }

        @Test
        fun `between times groupings`() {
            val document1 = aDocument()
            val document2 = aDocument()
            val document3 = aDocument()

            crux.submitTx {
                + document1 from dates[0]

                between(dates[1], dates[5]) {
                    + document2
                    + document3
                }
            }.await()

            crux.assert {
                at(dates[0]) {
                    + document1
                    - document2
                    - document3
                }

                at(dates[1]) {
                    + document2
                    + document3
                }

                at(dates[4]) {
                    + document2
                    + document3
                }

                at(dates[5]) {
                    - document2
                    - document3
                }
            }

            crux.submitTx {
                - document1.id from dates[1]

                between(dates[2], dates[3]) {
                    - document2.id
                    - document3.id
                }
            }.await()

            crux.assert {
                document1 notAt dates[1]

                at(dates[2]) {
                    - document2
                    - document3
                }

                at(dates[3]) {
                    - document1
                    + document2
                    + document3
                }
            }
        }
    }

    @Nested
    inner class DirectComparisons {
        private fun assert(java: Transaction, kotlin: Transaction) = assertEquals(java, kotlin)

        // These are here for convenience
        private val document = aDocument()
        private val document1 = aDocument()
        private val document2 = aDocument()
        private val document3 = aDocument()
        private val document4 = aDocument()
        private val document5 = aDocument()

        @Test
        fun `put a document`() =
            assert(
                buildTx {
                    it.put(document)
                },
                build {
                    + document
                }
            )

        @Test
        fun `put a document at time`() =
            assert(
                buildTx {
                    it.put(document, dates[1])
                },
                build {
                    + document from dates[1]
                }
            )

        @Test
        fun `put a document with end valid time`() =
            assert(
                buildTx {
                    it.put(document, dates[1], dates[2])
                },
                build {
                    + document from dates[1] until dates[2]
                }
            )

        @Test
        fun `delete a document`() =
            assert(
                buildTx {
                    it.delete(document.id)
                },
                build {
                    - document.id
                }
            )

        @Test
        fun `delete a document at valid time`() =
            assert(
                buildTx {
                    it.delete(document.id, dates[1])
                },
                build {
                    - document.id from dates[1]
                }
            )

        @Test
        fun `delete a document with end valid time`() =
            assert(
                buildTx {
                    it.delete(document.id, dates[1], dates[2])
                },
                build {
                    - document.id from dates[1] until dates[2]
                }
            )

        @Test
        fun `evict a document`() =
            assert(
                buildTx {
                    it.evict(document.id)
                },
                build {
                    evict(document.id)
                }
            )

        @Test
        fun `match a document`() =
            assert(
                buildTx {
                    it.match(document1)
                    it.put(document2)
                },
                build {
                    match(document1)
                    + document2
                }
            )

        @Test
        fun `match a document at a valid time`() =
            assert(
                buildTx {
                    it.match(document1, dates[0])
                    it.put(document2)
                },
                build {
                    match(document1) at dates[0]
                    + document2
                }
            )

        @Test
        fun `match against no document`() =
            assert(
                buildTx {
                    it.matchNotExists(document1.id)
                    it.put(document2)
                },
                build {
                    notExists(document1.id)
                    + document2
                }
            )

        @Test
        fun `match against a no document at valid time`() =
            assert(
                buildTx {
                    it.matchNotExists(document1.id, dates[1])
                    it.put(document)
                },
                build {
                    notExists(document1.id) at dates[1]
                    + document
                }
            )

        @Test
        fun `a more complicated example`() =
            assert(
                buildTx {
                    it.put(document1, dates[3])
                    it.put(document2, dates[2])
                    it.put(document3, dates[2], dates[4])
                    it.delete(document4.id, dates[2])
                    it.match(document2)
                    it.evict(document3.id)
                    it.matchNotExists(document5.id)
                    it.match(document4, dates[7])
                    it.put(document5, dates[3], dates[6])
                    it.delete(document2.id, dates[3], dates[6])
                },
                build {
                    + document1 from dates[3]

                    from(dates[2]) {
                        + document2
                        + document3 until dates[4]
                        - document4.id
                    }

                    match(document2)
                    evict(document3.id)
                    notExists(document5.id)
                    match(document4) at dates[7]

                    between(dates[3], dates[6]) {
                        + document5
                        - document2.id
                    }
                }
            )

    }
}