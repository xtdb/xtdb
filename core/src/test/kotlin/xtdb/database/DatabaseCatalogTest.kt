package xtdb.database

import clojure.lang.Keyword
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.asCoroutineDispatcher
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.InternalApi
import xtdb.NodeBase
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.error.Anomaly
import xtdb.api.error.Conflict
import xtdb.api.error.Incorrect
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.TxIndexer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

class DatabaseCatalogTest {

    private val ERROR_CODE = Keyword.intern("xtdb.error", "code")

    private fun Anomaly.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")

    @Test
    fun `reattach during detach returns transient conflict (#5613)`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            // Pin the closer to a single-thread dispatcher whose one thread we hold with `gate`, so the
            // detaching database is held mid-teardown for as long as we need to observe the conflict.
            // The teardown otherwise completes on a background thread and would race the re-attach to
            // win the observation.
            val gate = CountDownLatch(1)
            val closerExecutor = Executors.newSingleThreadExecutor()
            try {
                closerExecutor.execute {
                    try { gate.await() } catch (e: InterruptedException) { Thread.currentThread().interrupt() }
                }

                DatabaseCatalog.open(base, closerExecutor.asCoroutineDispatcher()).use { catalog ->
                    catalog.attach("test_db", Database.Config())
                    try {
                        // The teardown coroutine is queued behind the gate, so the entry stays Detaching
                        // and the re-attach sees the transient conflict.
                        catalog.detach("test_db")

                        val ex = assertThrows<Conflict> {
                            catalog.attach("test_db", Database.Config())
                        }
                        assertEquals("xtdb/db-being-detached", ex.errCode())
                    } finally {
                        // Release before `use` closes the catalog — close() joins the closer's children.
                        gate.countDown()
                    }

                    // With teardown released, the name frees up and re-attach eventually succeeds.
                    val deadline = System.nanoTime() + SECONDS.toNanos(10)
                    while (true) {
                        try {
                            catalog.attach("test_db", Database.Config()); break
                        } catch (e: Conflict) {
                            check(System.nanoTime() < deadline) { "detach did not complete within 10s" }
                            Thread.sleep(10)
                        }
                    }
                }
            } finally {
                gate.countDown()
                closerExecutor.shutdownNow()
            }
        }
    }

    @Test
    fun `attaching a multi-partition database is refused`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base).use { catalog ->
                val ex = assertThrows<Incorrect> {
                    catalog.attach("test_db", Database.Config(partitions = 4))
                }
                assertEquals("xtdb/multi-partition-not-yet-enabled", ex.errCode())

                assertFalse("test_db" in catalog.databaseNames, "a refused attach leaves no entry behind")
            }
        }
    }

    @OptIn(InternalApi::class)
    @Test
    fun `a source that declares enough partitions may be attached with them`() {
        val factory = object : ExternalSource.Factory {
            override val maxPartitions = 4

            override fun open(dbName: String, remotes: Map<RemoteAlias, Remote>, meterRegistry: MeterRegistry?) =
                object : ExternalSource {
                    override suspend fun onPartitionAssigned(
                        partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer
                    ) = awaitCancellation()

                    override fun close() = Unit
                }
        }

        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base).use { catalog ->
                catalog.attach("test_db", Database.Config(partitions = 4).externalSource(factory))

                assertEquals(4, catalog.databaseOrNull("test_db")!!.partitions.size)
            }
        }
    }
}
