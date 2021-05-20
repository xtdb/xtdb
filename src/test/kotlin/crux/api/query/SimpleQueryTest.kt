package crux.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.query.context.q
import crux.api.underware.kw
import crux.api.underware.sym
import crux.api.tx.submitTx
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.simplifiedResultSet
import utils.singleResultSet
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimpleQueryTest {
    companion object {
        private val person = "person".sym
        private val name = "name".sym
        private val forename = "forename".sym
        private val surname = "surname".sym

        private val forenameKey = "forename".kw
        private val surnameKey = "surname".kw

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
            +ivan
            +ivana
            +petr
            +mirror
            +clint
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `existence of fields`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has forenameKey
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
                    +person
                }

                where {
                    person has forenameKey eq "Ivan"
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
                    +person
                    +name
                }

                where {
                    person has forenameKey eq name
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
                    +person
                }

                where {
                    person has forenameKey eq name
                    person has surnameKey eq name
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
                    +person
                }

                where {
                    person has surnameKey eq "Ivanov"

                    not {
                        person has forenameKey eq "Ivan"
                    }
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivana")
            )
        )

    @Test
    fun `using an or block`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    or {
                        person has forenameKey eq "Ivan"
                        person has forenameKey eq "Petr"
                    }
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "petr")
            )
        )

    @Test
    fun `simple binding`() =
        assertThat(
            db.q("Ivan") {
                find {
                    +person
                }

                bind {
                    +name
                }

                where {
                    person has forenameKey eq name
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan")
            )
        )

    @Test
    fun `collection binding`() =
        assertThat(
            db.q(listOf("Ivan", "Petr")) {
                find {
                    +person
                }

                bind {
                    col(name)
                }

                where {
                    person has forenameKey eq name
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "petr")
            )
        )

    @Test
    fun `multiple bindings`() =
        assertThat(
            db.q("Ivan", "Ivanov") {
                find {
                    +person
                }

                bind {
                    +forename
                    +surname
                }

                where {
                    person has forenameKey eq forename
                    person has surnameKey eq surname
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan")
            )
        )

    @Test
    fun `multiple collection bindings`() =
        assertThat(
            db.q(
                listOf("Ivan", "Petr"),
                listOf("Ivanov", "Petrov")
            ) {
                find {
                    +person
                }

                bind {
                    col(forename)
                    col(surname)
                }

                where {
                    person has forenameKey eq forename
                    person has surnameKey eq surname
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "petr")
            )
        )
}