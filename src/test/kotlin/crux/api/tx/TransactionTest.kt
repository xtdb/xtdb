package crux.api.tx

import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.TransactionInstant
import crux.api.query.domain.CruxDocumentSerde
import crux.api.tx.Transaction.*
import crux.api.tx.TransactionContext.Companion.build
import org.junit.jupiter.api.AfterAll
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

    @AfterAll
    fun afterAll() {
        crux.close()
    }

    @Nested
    inner class BaseJavaCrux {
        /**
         * This is for testing that all is well with java and providing a comparison with our new DSL
         */

        @Test
        fun `can put a document`() = crux.run {
            val document = aDocument()

            submitTx(
                buildTx {
                    it.put(document)
                }
            ).await()

            assert {
                +document
            }
        }

        @Test
        fun `can put a document at time`() = crux.run {
            val document = aDocument()

            submitTx(
                buildTx {
                    it.put(document, dates[1])
                }
            ).await()

            assert {
                document notAt dates[0]
                document at dates[1]
            }
        }

        @Test
        fun `can put a document with end valid time`() = crux.run {
            val document = aDocument()

            submitTx(
                buildTx {
                    it.put(document, dates[1], dates[2])
                }
            ).await()

            assert {
                document notAt dates[0]
                document at dates[1]
                document notAt dates[2]
            }
        }

        @Test
        fun `can delete a document`() = crux.run {
            val document = aDocument()
            
            putAndWait(document)

            submitTx(
                buildTx {
                    it.delete(document.id)
                }
            ).await()

            assert {
                -document
            }
        }

        @Test
        fun `can delete a document at valid time`() = crux.run {
            val document = aDocument()
            
            putAndWait(document, dates[0])

            submitTx (
                buildTx {
                    it.delete(document.id, dates[1])
                }
            ).await()

            assert {
                document at dates[0]
                document notAt dates[1]
            }
        }

        @Test
        fun `can delete a document with end valid time`() = crux.run {
            val document = aDocument()
            
            putAndWait(document, dates[0])

            submitTx (
                buildTx {
                    it.delete(document.id, dates[1], dates[2])
                }
            ).await()

            assert {
                document at dates[0]
                document notAt dates[1]
                document at dates[2]
            }
        }

        @Test
        fun `can evict a document`() = crux.run {
            val document = aDocument()
            
            putAndWait(document)

            submitTx (
                buildTx {
                    it.evict(document.id)
                }
            ).await()

            assert {
                -document
            }
        }

        @Test
        fun `can match a document`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            submitTx (
                buildTx {
                    it.match(matchDoc)
                    it.put(document)
                }
            ).await()

            assert {
                -document
            }

            putAndWait(matchDoc)

            submitTx (
                buildTx {
                    it.match(matchDoc)
                    it.put(document)
                }
            ).await()

            assert {
                +document
            }
        }

        @Test
        fun `can match a document at a valid time`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            putAndWait(matchDoc, dates[1])

            submitTx (
                buildTx {
                    it.match(matchDoc, dates[0])
                    it.put(document)
                }
            ).await()

            assert {
                -document
            }

            submitTx (
                buildTx {
                    it.match(matchDoc, dates[1])
                    it.put(document)
                }
            ).await()

            assert {
                +document
            }
        }

        @Test
        fun `can match against no document`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            putAndWait(matchDoc)

            submitTx (
                buildTx {
                    it.matchNotExists(matchDoc.id)
                    it.put(document)
                }
            ).await()

            assert {
                -document
            }

            submitTx (
                buildTx {
                    it.matchNotExists(UUID.randomUUID().toString())
                    it.put(document)
                }
            ).await()

            assert {
                +document
            }
        }

        @Test
        fun `can match against a no document at valid time`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            putAndWait(matchDoc, dates[1])

            submitTx(
                buildTx {
                    it.matchNotExists(matchDoc.id, dates[1])
                    it.put(document)
                }
            ).await()

            assert {
                -document
            }

            submitTx(
                buildTx {
                    it.matchNotExists(matchDoc.id, dates[0])
                    it.put(document)
                }
            ).await()

            assert {
                +document
            }
        }
    }

    @Nested
    inner class KotlinDSL {
        @Test
        fun `can put a document`() = crux.run {
            val document = aDocument()

            submitTx {
                put(document)
            }.await()

            assert {
                +document
            }
        }

        @Test
        fun `can put a document at time`() = crux.run {
            val document = aDocument()
            submitTx {
                put(document from dates[1])
            }.await()

            assert {
                document notAt dates[0]
                document at dates[1]
            }
        }

        @Test
        fun `can put a document with end valid time`() = crux.run {
            val document = aDocument()

            submitTx{
                put(document from dates[1] until dates[2])
            }.await()

            assert {
                document notAt dates[0]
                document at dates[1]
                document notAt dates[2]
            }
        }

        @Test
        fun `can delete a document`() = crux.run {
            val document = aDocument()
            putAndWait(document)

            submitTx {
                delete(document.id)
            }.await()

            assert {
                -document
            }
        }

        @Test
        fun `can delete a document at valid time`() = crux.run {
            val document = aDocument()

            putAndWait(document, dates[0])

            submitTx {
                delete(document.id from dates[1])
            }.await()

            assert {
                document at dates[0]
                document notAt dates[1]
            }
        }

        @Test
        fun `can delete a document with end valid time`() = crux.run {
            val document = aDocument()

            putAndWait(document, dates[0])

            submitTx {
                delete(document.id from dates[1] until dates[2])
            }.await()

            assert {
                document at dates[0]
                document notAt dates[1]
                document at dates[2]
            }
        }

        @Test
        fun `can evict a document`() = crux.run {
            val document = aDocument()

            putAndWait(document)

            submitTx {
                evict(document.id)
            }.await()

            assert {
                -document
            }
        }

        @Test
        fun `can match a document`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            submitTx {
                match(matchDoc)
                put(document)
            }.await()

            assert {
                -document
            }

            putAndWait(matchDoc)

            submitTx {
                match(matchDoc)
                put(document)
            }.await()

            assert {
                +document
            }
        }

        @Test
        fun `can match a document at a valid time`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            putAndWait(matchDoc, dates[1])

            submitTx {
                match(matchDoc at dates[0])
                put(document)
            }.await()

            assert {
                -document
            }

            submitTx {
                match(matchDoc at dates[1])
                put(document)
            }.await()

            assert {
                +document
            }
        }

        @Test
        fun `can match against no document`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            putAndWait(matchDoc)

            submitTx {
                notExists(matchDoc.id)
                put(document)
            }.await()

            assert {
                -document
            }

            submitTx {
                notExists(UUID.randomUUID().toString())
                put(document)
            }.await()

            assert {
                +document
            }
        }

        @Test
        fun `can match against a no document at valid time`() = crux.run {
            val matchDoc = aDocument()
            val document = aDocument()

            putAndWait(matchDoc, dates[1])

            submitTx {
                notExists(matchDoc.id at dates[1])
                put(document)
            }.await()

            assert {
                -document
            }

            submitTx {
                notExists(matchDoc.id at dates[0])
                put(document)
            }.await()

            assert {
                +document
            }
        }
    }

    @Nested
    inner class TransactionDslGroupings {
        @Test
        fun `valid time groupings`() = crux.run {
            val document1 = aDocument()
            val document2 = aDocument()
            val document3 = aDocument()

            submitTx {
                put(document1 from dates[0])

                from(dates[1]) {
                    put(document2)
                    put(document3 until dates[2])
                }
            }.await()

            assert {
                at(dates[0]) {
                    +document1
                    -document2
                    -document3
                }

                at(dates[1]) {
                    +document2
                    +document3
                }

                at(dates[2]) {
                    +document2
                    -document3
                }
            }

            submitTx {
                from(dates[3]) {
                    delete(document1.id)
                    delete(document2.id until dates[4])
                }
            }.await()

            assert {
                at(dates[2]) {
                    +document1
                    +document2
                }

                at(dates[3]) {
                    -document1
                    -document2
                }

                at(dates[4]) {
                    -document1
                    +document2
                }
            }
        }

        @Test
        fun `between times groupings`() = crux.run {
            val document1 = aDocument()
            val document2 = aDocument()
            val document3 = aDocument()

            submitTx {
                put(document1 from dates[0])

                between(dates[1], dates[5]) {
                    put(document2)
                    put(document3)
                }
            }.await()

            assert {
                at(dates[0]) {
                    +document1
                    -document2
                    -document3
                }

                at(dates[1]) {
                    +document2
                    +document3
                }

                at(dates[4]) {
                    +document2
                    +document3
                }

                at(dates[5]) {
                    -document2
                    -document3
                }
            }

            submitTx {
                delete(document1.id from dates[1])

                between(dates[2], dates[3]) {
                    delete(document2.id)
                    delete(document3.id)
                }
            }.await()

            assert {
                document1 notAt dates[1]

                at(dates[2]) {
                    -document2
                    -document3
                }

                at(dates[3]) {
                    -document1
                    +document2
                    +document3
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
                    put(document)
                }
            )

        @Test
        fun `put a document at time`() =
            assert(
                buildTx {
                    it.put(document, dates[1])
                },
                build {
                    put(document from dates[1])
                }
            )

        @Test
        fun `put a document with end valid time`() =
            assert(
                buildTx {
                    it.put(document, dates[1], dates[2])
                },
                build {
                    put(document from dates[1] until dates[2])
                }
            )

        @Test
        fun `delete a document`() =
            assert(
                buildTx {
                    it.delete(document.id)
                },
                build {
                    delete(document.id)
                }
            )

        @Test
        fun `delete a document at valid time`() =
            assert(
                buildTx {
                    it.delete(document.id, dates[1])
                },
                build {
                    delete(document.id from dates[1])
                }
            )

        @Test
        fun `delete a document with end valid time`() =
            assert(
                buildTx {
                    it.delete(document.id, dates[1], dates[2])
                },
                build {
                    delete(document.id from dates[1] until dates[2])
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
                    put(document2)
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
                    match(document1 at dates[0])
                    put(document2)
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
                    put(document2)
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
                    notExists(document1.id at dates[1])
                    put(document)
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
                    put(document1 from dates[3])

                    from(dates[2]) {
                        put(document2)
                        put(document3 until dates[4])
                        delete(document4.id)
                    }

                    match(document2)
                    evict(document3.id)
                    notExists(document5.id)
                    match(document4 at dates[7])

                    between(dates[3], dates[6]) {
                        put(document5)
                        delete(document2.id)
                    }
                }
            )
    }

    data class Person(val id: String, val forename: String, val surname: String)
    object CruxPerson : CruxDocumentSerde<Person> {
        private const val FORENAME = "forename"
        private const val SURNAME = "surname"

        override fun toDocument(obj: Person): CruxDocument =
            CruxDocument.build(obj.id) {
                it.put(FORENAME, obj.forename)
                it.put(SURNAME, obj.surname)
            }

        override fun toObject(document: CruxDocument): Person =
            Person(
                document.id as String,
                document.get(FORENAME) as String,
                document.get(SURNAME) as String
            )
    }

    @Nested
    inner class CruxDocumentSerdeTests {
        private fun aPersonWithDocument(): Pair<Person, CruxDocument> {
            val id = UUID.randomUUID().toString()
            return Pair(
                Person(
                    id,
                    "Alistair",
                    "O'Neill"
                ),
                CruxDocument.build(id) {
                    it.put("forename", "Alistair")
                    it.put("surname", "O'Neill")
                }
            )
        }


        @Test
        fun `can put a person`() = crux.run {
            val (person, document) = aPersonWithDocument()

            submitTx {
                put(person by CruxPerson)
            }.await()

            assert {
                +document
            }
        }

        @Test
        fun `can put a person at time`() = crux.run {
            val (person, document) = aPersonWithDocument()
            submitTx {
                put(person by CruxPerson from dates[1])
            }.await()

            assert {
                document notAt dates[0]
                document at dates[1]
            }
        }

        @Test
        fun `can put a document with end valid time`() = crux.run {
            val (person, document) = aPersonWithDocument()

            submitTx{
                put(person by CruxPerson from dates[1] until dates[2])
            }.await()

            assert {
                document notAt dates[0]
                document at dates[1]
                document notAt dates[2]
            }
        }
    }
}