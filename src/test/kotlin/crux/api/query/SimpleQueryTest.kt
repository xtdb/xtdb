package crux.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.kw
import crux.api.sym
import crux.api.tx.submitTx
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.simplifiedResultSet
import utils.singleResultSet
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimpleQueryTest {
    companion object {
        private fun createPerson(key: String, forename: String, surname: String) =
            CruxDocument.build(key) {
                it.put("forename", forename)
                it.put("surname", surname)
            }

        private val ivan = createPerson("ivan", "Ivan", "Ivanov")
        private val ivana = createPerson("ivana", "Ivana", "Ivanov")
        private val petr = createPerson("petr", "Petr", "Petrov")

        // For testing equality predicate
        private val mirror = createPerson("mirror", "Mirror", "Mirror")

        // The man with no name
        private val clint = CruxDocument.build("clint") {}
    }

    private val db = CruxK.startNode().apply {
        submitTx {
            + ivan
            + ivana
            + petr
            + mirror
            + clint
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    private val p = "p".sym
    private val n = "n".sym

    private val forename = "forename".kw
    private val surname = "surname".kw



    @Test
    fun `existence of fields`() =
        assertThat(
            db.q {
                find {
                    + p
                }

                where {
                    p has forename
                }
            }.singleResultSet(),
            equalTo(
                setOf(
                    "ivan",
                    "ivana",
                    "petr",
                    "mirror"
                )
            )
        )

    @Test
    fun `equality of fields`() =
        assertThat(
            db.q {
                find {
                    + p
                }

                where {
                    p has forename eq "Ivan"
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan")
            )
        )

    @Test
    fun `returning more than one value`() =
        assertThat(
            db.q {
                find {
                    + p
                    + n
                }

                where {
                    p has forename eq n
                }
            }.simplifiedResultSet(),
            equalTo(
                setOf(
                    listOf("ivan", "Ivan"),
                    listOf("ivana", "Ivana"),
                    listOf("petr", "Petr"),
                    listOf("mirror", "Mirror")
                )
            )
        )

    @Test
    fun `joining on fields`() =
        assertThat(
            db.q {
                find {
                    + p
                }

                where {
                    p has forename eq n
                    p has surname eq n
                }
            }.singleResultSet(),
            equalTo(
                setOf("mirror")
            )
        )

    @Test
    fun `negating where clauses`() =
        assertThat (
            db.q {
                find {
                    + p
                }

                where {
                    p has surname eq "Ivanov"

                    not {
                        p has forename eq "Ivan"
                    }
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivana")
            )
        )

    @Test
    fun `using an or block`() =
        assertThat (
            db.q {
                find {
                    + p
                }

                where {
                    or {
                        p has forename eq "Ivan"
                        p has forename eq "Petr"
                    }
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "petr")
            )
        )
}