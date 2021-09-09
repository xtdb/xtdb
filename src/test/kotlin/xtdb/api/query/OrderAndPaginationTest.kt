package xtdb.api.query

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import utils.simplifiedResultList
import utils.singleResultList
import xtdb.api.XtdbDocument
import xtdb.api.XtdbKt
import xtdb.api.query.conversion.q
import xtdb.api.tx.submitTx
import xtdb.api.underware.kw
import xtdb.api.underware.sym
import java.time.Duration

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrderAndPaginationTest {
    companion object {
        private val entry = "entry".sym
        private val temperature = "temperature".sym
        private val humidity = "humidity".sym

        private val temperatureKey = "temperature".kw
        private val humidityKey = "humidity".kw
    }

    private val documents =
        (0 until 100)
            .map(Int::toLong) // XTDB will spit out longs
            .map {
                XtdbDocument.build(it) { b ->
                    b.put("temperature", it)
                    b.put("humidity", it/2 +(it%2)*50) // Bijective to (0 until 100) but in a different order
                }
            }

    private val db = XtdbKt.startNode().apply {
        submitTx {
            // Insert documents shuffled to avoid passing tests by internal ordering convenience
            documents.shuffled().forEach { put(it) }
        }.also {
            awaitTx(it, Duration.ofSeconds(10))
        }
    }.db()

    @Test
    fun `ordering by ascending works`() =
        assertThat(
            db.q {
                find {
                    +temperature
                }

                where {
                    entry has temperatureKey eq temperature
                }

                order {
                    +temperature
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
                    +temperature
                }

                where {
                    entry has temperatureKey eq temperature
                }

                order {
                    -temperature
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
                    +temperature
                    +humidity
                }

                where {
                    entry has temperatureKey eq temperature
                    entry has humidityKey eq humidity
                }

                order {
                    +temperature
                    -humidity
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

    @Test
    fun `offset works as expected`() {
        assertThat(
            db.q {
                find {
                    +temperature
                }

                where {
                    entry has temperatureKey eq temperature
                }

                order {
                    +temperature
                }

                offset = 15
            }.singleResultList(),
            equalTo(
                documents
                    .map { it.get("temperature") as Long }
                    .sorted()
                    .drop(15)
            )
        )
    }

    @Test
    fun `limit works as expected`() {
        assertThat(
            db.q {
                find {
                    +temperature
                }

                where {
                    entry has temperatureKey eq temperature
                }

                order {
                    +temperature
                }

                limit = 15
            }.singleResultList(),
            equalTo(
                documents
                    .map { it.get("temperature") as Long }
                    .sorted()
                    .take(15)
            )
        )
    }

    @Test
    fun `offset and limit work together`() {
        assertThat(
            db.q {
                find {
                    +temperature
                }

                where {
                    entry has temperatureKey eq temperature
                }

                order {
                    +temperature
                }

                offset = 10
                limit = 15
            }.singleResultList(),
            equalTo(
                documents
                    .map { it.get("temperature") as Long }
                    .sorted()
                    .drop(10)
                    .take(15)
            )
        )
    }
}
