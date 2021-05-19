package crux.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.CruxK
import crux.api.sym
import crux.api.tx.submitTx
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryTest {
    companion object {
        private fun createPerson(key: String, forename: String, surname: String) =
            CruxDocument.build(key) {
                it.put("forename", forename)
                it.put("surname", surname)
            }

        private val ivan = createPerson("ivan", "Ivan", "Ivanov")
        private val petr = createPerson("petr", "Petr", "Petrov")

        // The man with no name
        private val clint = CruxDocument.build("clint") {}
    }

    private val crux = CruxK.startNode().apply {
        submitTx {
            + ivan
            + petr
            + clint
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }

    @Nested
    inner class SimpleQueries {

        @Test
        fun `existence of fields`() =
            assertThat(
                crux.db().use { db ->
                    val p = "p".sym

                    db.q {
                        find {
                            + p
                        }

                        where {
                            p has "forename"
                        }
                    }
                }
                    .map { it[0] }
                    .toSet(),
                equalTo(
                    setOf(
                        "ivan",
                        "petr"
                    )
                )
            )
    }
}