package crux.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.query.conversion.q
import crux.api.tx.submitTx
import crux.api.underware.kw
import crux.api.underware.sym
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.simplifiedResultSet
import utils.singleResultSet
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PullTest {
    companion object {
        private val nameKey = "name".kw
        private val professionKey = "profession".kw

        private fun createPerson(id: String, name: String, profession: String) = CruxDocument.build(id) {
            it.put("name", name)
            it.put("profession", profession)
        }

        private fun createProfession(id: String, name: String) = CruxDocument.build(id) {
            it.put("name", name)
        }

        val person = "person".sym
        val name = "name".sym
        val profession = "profession".sym

        val cruxId = "crux.db/id".kw
    }

    private val db = CruxK.startNode().apply {
        submitTx {
            put(createPerson("ivan", "Ivan", "doctor"))
            put(createPerson("sergei", "Sergei", "lawyer"))
            put(createPerson("petr", "Petr", "doctor"))
            put(createProfession("doctor", "Doctor"))
            put(createProfession("lawyer", "Lawyer"))
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `check data is set up correctly`() =
        assertThat(
            db.q {
                find {
                    +person
                    +name
                    +profession
                }

                where {
                    person has nameKey eq name
                    person has professionKey eq profession
                }
            }.simplifiedResultSet(),
            equalTo(
                setOf(
                    listOf("ivan", "Ivan", "doctor"),
                    listOf("petr", "Petr", "doctor"),
                    listOf("sergei", "Sergei", "lawyer")
                )
            )
        )

    @Test
    fun `pulling with specific values`() =
        assertThat(
            db.q {
                find {
                    pull(person) {
                        +nameKey
                        +professionKey
                    }
                }

                where {
                    person has professionKey
                }
            }.singleResultSet(),
            equalTo(
                setOf(
                    mapOf(
                        nameKey to "Ivan",
                        professionKey to "doctor"
                    ),
                    mapOf(
                        nameKey to "Petr",
                        professionKey to "doctor"
                    ),
                    mapOf(
                        nameKey to "Sergei",
                        professionKey to "lawyer"
                    )
                )
            )
        )

    @Test
    fun `pulling all`() =
        assertThat(
            db.q {
                find {
                    pullAll(person)
                }

                where {
                    person has professionKey
                }
            }.singleResultSet(),
            equalTo(
                setOf(
                    mapOf(
                        cruxId to "ivan",
                        nameKey to "Ivan",
                        professionKey to "doctor"
                    ),
                    mapOf(
                        cruxId to "petr",
                        nameKey to "Petr",
                        professionKey to "doctor"
                    ),
                    mapOf(
                        cruxId to "sergei",
                        nameKey to "Sergei",
                        professionKey to "lawyer"
                    )
                )
            )
        )

    @Test
    fun `can rename key with pull`() =
        assertThat(
            db.q {
                find {
                    pull(person) {
                        nameKey with {
                            name = "MySpecialKey".kw
                        }
                    }
                }

                where {
                    person has professionKey eq "lawyer"
                }
            }.singleResultSet(),
            equalTo(
                setOf(
                    mapOf(
                        "MySpecialKey".kw to "Sergei"
                    )
                )
            )
        )

    @Test
    fun `can provide default with pull`() =
        assertThat(
            db.q {
                find {
                    pull(person) {
                        "NonExistantKey".kw with {
                            default = "DefaultValue"
                        }
                    }
                }

                where {
                    person has professionKey eq "lawyer"
                }
            }.singleResultSet(),
            equalTo(
                setOf(
                    mapOf(
                        "NonExistantKey".kw to "DefaultValue"
                    )
                )
            )
        )
}