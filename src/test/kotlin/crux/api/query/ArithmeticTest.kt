package crux.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.query.conversion.q
import crux.api.tx.submitTx
import crux.api.underware.kw
import crux.api.underware.sym
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.singleResult
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArithmeticTest {
    companion object {
        private fun createDocument(name: String, value: Int) = CruxDocument.build(name) {
            it.put("name", name)
            it.put("value", value)
        }

        private val name = "name".kw
        private val value = "value".kw
        private val six = "six".sym
        private val three = "three".sym
        private val n = "n".sym
        private val m = "m".sym
        private val result = "result".sym
    }

    private val db = CruxK.startNode().apply {
        submitTx {
            put(createDocument("six", 6))
            put(createDocument("three", 3))
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Nested
    inner class Addition {
        @Test
        fun `can add a symbol and a number`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        n + 2 eq result
                    }
                }.singleResult(),
                equalTo(8L)
            )
        }

        @Test
        fun `can add a symbol and a symbol`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        three has name eq "three"
                        three has value eq m
                        n + m eq result
                    }
                }.singleResult(),
                equalTo(9L)
            )
        }
    }

    @Nested
    inner class Subtraction {
        @Test
        fun `can subtract a symbol and a number`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        n - 2 eq result
                    }
                }.singleResult(),
                equalTo(4L)
            )
        }

        @Test
        fun `can subtract a symbol and a symbol`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        three has name eq "three"
                        three has value eq m
                        n - m eq result
                    }
                }.singleResult(),
                equalTo(3L)
            )
        }
    }

    @Nested
    inner class Multiplication {
        @Test
        fun `can multiply a symbol and a number`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        n * 2 eq result
                    }
                }.singleResult(),
                equalTo(12L)
            )
        }

        @Test
        fun `can multiply a symbol and a symbol`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        three has name eq "three"
                        three has value eq m
                        n * m eq result
                    }
                }.singleResult(),
                equalTo(18L)
            )
        }
    }

    @Nested
    inner class Division {
        @Test
        fun `can divide a symbol and a number`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        n / 2 eq result
                    }
                }.singleResult(),
                equalTo(3L)
            )
        }

        @Test
        fun `can divide a symbol and a symbol`() {
            assertThat(
                db.q {
                    find {
                        + result
                    }

                    where {
                        six has name eq "six"
                        six has value eq n
                        three has name eq "three"
                        three has value eq m
                        n / m eq result
                    }
                }.singleResult(),
                equalTo(2L)
            )
        }
    }
}