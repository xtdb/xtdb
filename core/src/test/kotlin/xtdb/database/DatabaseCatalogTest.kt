package xtdb.database

import clojure.lang.Keyword
import kotlinx.coroutines.asCoroutineDispatcher
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase
import xtdb.api.error.Conflict
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

class DatabaseCatalogTest {

    private val ERROR_CODE = Keyword.intern("xtdb.error", "code")

    private fun Conflict.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")

    @Test
    fun `concurrent attaches of one name open it once, and refuse the rest`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base).use { catalog ->
                val attachers = 8
                val start = CountDownLatch(1)
                val executor = Executors.newFixedThreadPool(attachers)
                try {
                    val outcomes = (1..attachers)
                        .map {
                            executor.submit<Result<Unit>> {
                                start.await()
                                runCatching { catalog.attach("test_db", Database.Config()) }
                            }
                        }
                        .also { start.countDown() }
                        .map { it.get(30, SECONDS) }

                    assertEquals(1, outcomes.count { it.isSuccess })
                    assertEquals(
                        List(attachers - 1) { "xtdb/db-exists" },
                        outcomes.mapNotNull { (it.exceptionOrNull() as? Conflict)?.errCode() }
                    )
                } finally {
                    executor.shutdownNow()
                }
            }
        }
    }

    @Test
    fun `concurrent first uses of one name all get the one database`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base).use { catalog ->
                val callers = 8
                val start = CountDownLatch(1)
                val executor = Executors.newFixedThreadPool(callers)
                try {
                    val dbs = (1..callers)
                        .map {
                            executor.submit<Database?> {
                                start.await()
                                catalog.databaseOrAttach("test_db")
                            }
                        }
                        .also { start.countDown() }
                        .map { it.get(30, SECONDS) }

                    val db = catalog.databaseOrNull("test_db")
                    assertNotNull(db)
                    assertEquals(List(callers) { db }, dbs)
                } finally {
                    executor.shutdownNow()
                }
            }
        }
    }

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
}
