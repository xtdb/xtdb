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
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AggregateTest {

    companion object {
        private data class Monster(val id: String, val heads: Int) {
            fun toDocument(): CruxDocument = CruxDocument.build(id) {
                it.put("heads", heads)
            }
        }

        private val monsters = listOf(
            Monster("cerberus", 3),
            Monster("medusa", 1),
            Monster("cyclops", 1),
            Monster("chimera", 1)
        )

        private val heads = "heads".sym
        private val monster = "monster".sym

        private val headsKey = "heads".kw
    }

    private val db = CruxK.startNode().apply {
        submitTx {
            monsters.forEach {
                + it.toDocument()
            }
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `aggregates work as expected`() =
        assertThat(
            db.q {
                find {
                    sum(heads)
                    min(heads)
                    max(heads)
                    count(heads)
                    distinct(heads)
                }

                where {
                    monster has headsKey eq heads
                }
            }.simplifiedResultSet(),
            equalTo(
                setOf(
                    listOf(6L, 1L, 3L, 4L, setOf(1L, 3L))
                )
            )
        )
}