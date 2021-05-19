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
import utils.simplifiedResultList
import utils.singleResultList
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrderAndPaginationTest {

    private val documents =
        (0 until 100)
            .map(Int::toLong) // Crux will spit out longs
            .map {
                CruxDocument.build(it) { b ->
                    b.put("temperature", it)
                    b.put("humidity", it/2 + (it%2)*50)
                }
            }

    private val db = CruxK.startNode().apply {
        submitTx {
            // Insert documents shuffled to avoid passing tests by internal ordering convenience
            documents.shuffled().forEach { +it }
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    private val d = "d".sym
    private val t = "t".sym
    private val h = "h".sym

    private val temperature = "temperature".kw
    private val humidity = "humidity".kw

    @Test
    fun `ordering by ascending works`() =
        assertThat(
            db.q {
                find {
                    + t
                }

                where {
                    d has temperature eq t
                }

                order {
                    + t
                }
            }.singleResultList(),
            equalTo(
                documents
                    .map { it.get("temperature") as Long }
                    .sorted()
            )
        )

    @Test
    fun `ordering by descending works`() =
        assertThat(
            db.q {
                find {
                    + t
                }

                where {
                    d has temperature eq t
                }

                order {
                    - t
                }
            }.singleResultList(),
            equalTo(
                documents
                    .map { it.get("temperature") as Long }
                    .sortedDescending()
            )
        )

    @Test
    fun `ordering by multiple parameters works`() =
        assertThat(
            db.q {
                find {
                    + t
                    + h
                }

                where {
                    d has temperature eq t
                    d has humidity eq h
                }

                order {
                    + t
                    - h
                }
            }.simplifiedResultList(),
            equalTo(
                documents
                    .map {
                        listOf(
                            it.get("temperature") as Long,
                            it.get("humidity") as Long
                        )
                    }
                    .sortedByDescending { it[1] }
                    .sortedBy { it[0] }
            )
        )

}