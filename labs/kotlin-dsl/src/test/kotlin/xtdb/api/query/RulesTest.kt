package xtdb.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.singleResultSet
import xtdb.api.XtdbDocument
import xtdb.api.XtdbKt
import xtdb.api.query.conversion.q
import xtdb.api.tx.submitTx
import xtdb.api.underware.kw
import xtdb.api.underware.sym
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RulesTest {
    companion object {
        private val gimli = XtdbDocument.build("gimli") {
            it.put("father", "gloin")
            it.put("age", 262L)
        }

        private val gloin = XtdbDocument.build("gloin") {
            it.put("father", "groin")
            it.put("age", 253L)
        }

        private val groin = XtdbDocument.build("groin") {
            it.put("father", "farin")
            it.put("age", 252L)
        }

        private val farin = XtdbDocument.build("farin") {
            it.put("father", "borin")
            it.put("age", 243L)
        }

        private val borin = XtdbDocument.build("borin") {
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

    private val db = XtdbKt.startNode().apply {
        submitTx {
            dwarves.forEach {
                put(it)
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

    @Test
    fun `recursive rules with bound variables`() {
        assertThat(
            db.q {
                find {
                    +dwarf
                }

                where {
                    rule(descendentOf) ("farin", dwarf)
                }

                rules {
                    def(descendentOf) [ancestor] (descendent) {
                        descendent has fatherKey eq ancestor
                    }

                    def(descendentOf) [ancestor] (descendent) {
                        descendent has fatherKey eq intermediate
                        rule(descendentOf) (ancestor, intermediate)
                    }
                }
            }.singleResultSet(),
            equalTo(
                setOf("gimli", "gloin", "groin")
            )
        )
    }
}