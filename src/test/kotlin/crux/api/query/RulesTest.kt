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
class RulesTest {
    companion object {
        private val gimli = CruxDocument.build("gimli") {
            it.put("father", "gloin")
            it.put("age", 262L)
        }

        private val gloin = CruxDocument.build("gloin") {
            it.put("father", "groin")
            it.put("age", 253L)
        }

        private val groin = CruxDocument.build("groin") {
            it.put("father", "farin")
            it.put("age", 252L)
        }

        private val farin = CruxDocument.build("farin") {
            it.put("father", "borin")
            it.put("age", 243L)
        }

        private val borin = CruxDocument.build("borin") {
            it.put("father", "nain2")
            it.put("age", 261L)
        }

        private val dwarves = listOf(gimli, gloin, groin, farin, borin)

        private val diedYoung = "diedYoung".sym
        private val descendentOf = "descendentOf".sym
        private val dwarf = "dwarf".sym
        private val age = "age".sym

        private val descendent = "descendent".sym
        private val ancestor = "anscestor".sym
        private val intermediate = "intermediate".sym

        private val fatherKey = "father".kw
        private val ageKey = "age".kw
    }

    private val db = CruxK.startNode().apply {
        submitTx {
            dwarves.forEach {
                +it
            }
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `simple predicate rule`() =
        assertThat(
            db.q {
                find {
                    +dwarf
                }

                where {
                    rule(diedYoung) (dwarf)
                }

                rules {
                    def(diedYoung) (dwarf) {
                        dwarf has ageKey eq age
                        age lte 250
                    }
                }
            }.singleResultSet(),
            equalTo(
                setOf("farin")
            )
        )

    @Test
    fun `recursive rules`() =
        assertThat(
            db.q {
                find {
                    +dwarf
                }

                where {
                    rule(descendentOf) (dwarf, "farin")
                }

                rules {
                    def(descendentOf) (descendent, ancestor) {
                        descendent has fatherKey eq ancestor
                    }

                    def(descendentOf) (descendent, ancestor) {
                        descendent has fatherKey eq intermediate
                        rule(descendentOf) (intermediate, ancestor)
                    }
                }
            }.singleResultSet(),
            equalTo(
                setOf("gimli", "gloin", "groin")
            )
        )
}