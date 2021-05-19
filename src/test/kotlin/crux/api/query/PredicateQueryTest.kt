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
import utils.singleResults
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PredicateQueryTest {
    companion object {
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
            + ivan
            + ivana
            + petr
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    private val p = "p".sym
    private val p1 = "p1".sym
    private val p2 = "p2".sym
    private val a = "a".sym
    private val a1 = "a1".sym
    private val a2 = "a2".sym

    private val name = "name".kw
    private val age = "age".kw

    @Test
    fun `greater than hardcoded`() =
        assertThat (
            db.q {
                find {
                    + p
                }

                where {
                    p has age eq a
                    a gt 28
                }
            }.singleResults(),
            equalTo(
                setOf("petr")
            )
        )

    @Test
    fun `greater than symbol`() =
        assertThat (
            db.q {
                find {
                    + p1
                }

                where {
                    p1 has age eq a1
                    p2 has name eq "Ivana"
                    p2 has age eq a2
                    a1 gt a2
                }
            }.singleResults(),
            equalTo(
                setOf("petr")
            )
        )

    @Test
    fun `greater than or equal to hardcoded`() =
        assertThat(
            db.q {
                find {
                    + p
                }

                where {
                    p has age eq a
                    a gte 28
                }
            }.singleResults(),
            equalTo(
                setOf("petr", "ivana")
            )
        )

    @Test
    fun `greater than or equal to symbol`() =
        assertThat(
            db.q {
                find {
                    + p1
                }

                where {
                    p1 has age eq a1
                    p2 has age eq a2
                    p2 has name eq "Ivana"
                    a1 gte a2
                }
            }.singleResults(),
            equalTo(
                setOf("petr", "ivana")
            )
        )

    @Test
    fun `less than hardcoded value`() =
        assertThat(
            db.q {
                find {
                    + p
                }

                where {
                    p has age eq a
                    a lt 28
                }
            }.singleResults(),
            equalTo(
                setOf("ivan")
            )
        )

    @Test
    fun `less than symbol`() =
        assertThat(
            db.q {
                find {
                    + p1
                }

                where {
                    p1 has age eq a1
                    p2 has age eq a2
                    p2 has name eq "Ivana"
                    a1 lt a2
                }
            }.singleResults(),
            equalTo(
                setOf("ivan")
            )
        )

    @Test
    fun `less than or equal to hardcoded value`() =
        assertThat(
            db.q {
                find {
                    + p
                }

                where {
                    p has age eq a
                    a lte 28
                }
            }.singleResults(),
            equalTo(
                setOf("ivan", "ivana")
            )
        )

    @Test
    fun `less than or equal to symbol`() =
        assertThat(
            db.q {
                find {
                    + p1
                }

                where {
                    p1 has age eq a1
                    p2 has age eq a2
                    p2 has name eq "Ivana"
                    a1 lte a2
                }
            }.singleResults(),
            equalTo(
                setOf("ivan", "ivana")
            )
        )
}