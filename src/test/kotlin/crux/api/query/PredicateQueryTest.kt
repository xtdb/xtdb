package crux.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.query.conversion.q
import crux.api.underware.kw
import crux.api.underware.sym
import crux.api.tx.submitTx
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.singleResultSet
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PredicateQueryTest {
    companion object {
        private val person = "person".sym
        private val person1 = "person1".sym
        private val person2 = "person2".sym
        private val age = "age".sym
        private val age1 = "age1".sym
        private val age2 = "age2".sym
        private val name = "name".sym

        private val nameKey = "name".kw
        private val ageKey = "age".kw

        private fun createPerson(key: String, name: String, age: Int) =
            CruxDocument.build(key) {
                it.put("name", name)
                it.put("age", age)
            }

        private val ivan = createPerson("ivan", "Ivan",27)
        private val ivana = createPerson("ivana", "Ivana",28)
        private val petr = createPerson("petr", "Petr", 29)
    }

    private val db = CruxK.startNode().apply {
        submitTx {
            +ivan
            +ivana
            +petr
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `greater than hardcoded`() =
        assertThat (
            db.q {
                find {
                    +person
                }

                where {
                    person has ageKey eq age
                    age gt 28
                }
            }.singleResultSet(),
            equalTo(
                setOf("petr")
            )
        )

    @Test
    fun `greater than symbol`() =
        assertThat (
            db.q {
                find {
                    +person1
                }

                where {
                    person1 has ageKey eq age1
                    person2 has nameKey eq "Ivana"
                    person2 has ageKey eq age2
                    age1 gt age2
                }
            }.singleResultSet(),
            equalTo(
                setOf("petr")
            )
        )

    @Test
    fun `greater than or equal to hardcoded`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has ageKey eq age
                    age gte 28
                }
            }.singleResultSet(),
            equalTo(
                setOf("petr", "ivana")
            )
        )

    @Test
    fun `greater than or equal to symbol`() =
        assertThat(
            db.q {
                find {
                    +person1
                }

                where {
                    person1 has ageKey eq age1
                    person2 has ageKey eq age2
                    person2 has nameKey eq "Ivana"
                    age1 gte age2
                }
            }.singleResultSet(),
            equalTo(
                setOf("petr", "ivana")
            )
        )

    @Test
    fun `less than hardcoded value`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has ageKey eq age
                    age lt 28
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan")
            )
        )

    @Test
    fun `less than symbol`() =
        assertThat(
            db.q {
                find {
                    +person1
                }

                where {
                    person1 has ageKey eq age1
                    person2 has ageKey eq age2
                    person2 has nameKey eq "Ivana"
                    age1 lt age2
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan")
            )
        )

    @Test
    fun `less than or equal to hardcoded value`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has ageKey eq age
                    age lte 28
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "ivana")
            )
        )

    @Test
    fun `less than or equal to symbol`() =
        assertThat(
            db.q {
                find {
                    +person1
                }

                where {
                    person1 has ageKey eq age1
                    person2 has ageKey eq age2
                    person2 has nameKey eq "Ivana"
                    age1 lte age2
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "ivana")
            )
        )

    @Test
    fun `equality to hardcoded number`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has ageKey eq age
                    age eq 28
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivana")
            )
        )

    @Test
    fun `equality to hardcoded string`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has nameKey eq name
                    name eq "Ivana"
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivana")
            )
        )

    @Test
    fun `equality to symbol`() =
        assertThat(
            db.q {
                find {
                    +person1
                }

                where {
                    person1 has ageKey eq age1
                    person2 has ageKey eq age2
                    person2 has nameKey eq "Ivana"
                    age1 eq age2
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivana")
            )
        )

    @Test
    fun `inequality to hardcoded number`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has ageKey eq age
                    age neq 28
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "petr")
            )
        )

    @Test
    fun `inequality to hardcoded string`() =
        assertThat(
            db.q {
                find {
                    +person
                }

                where {
                    person has nameKey eq name
                    name neq "Ivana"
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "petr")
            )
        )

    @Test
    fun `inequality to symbol`() =
        assertThat(
            db.q {
                find {
                    +person1
                }

                where {
                    person1 has ageKey eq age1
                    person2 has ageKey eq age2
                    person2 has nameKey eq "Ivana"
                    age1 neq age2
                }
            }.singleResultSet(),
            equalTo(
                setOf("ivan", "petr")
            )
        )
}