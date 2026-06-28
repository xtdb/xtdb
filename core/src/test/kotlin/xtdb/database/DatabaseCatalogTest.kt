package xtdb.database

import clojure.lang.Keyword
import kotlinx.coroutines.Dispatchers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase
import xtdb.error.Conflict
import java.util.concurrent.TimeUnit.SECONDS

class DatabaseCatalogTest {

    private val ERROR_CODE = Keyword.intern("xtdb.error", "code")

    private fun Conflict.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")

    @Test
    fun `reattach during detach returns transient conflict (#5613)`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            // Unconfined runs the detach inline up to its first real suspension — the suspend
            // cancelAndJoin of the database's job tree — so the database is still mid-teardown
            // (isClosing, not yet removed) when we observe the conflict, with no scheduler poking.
            DatabaseCatalog.open(base, Dispatchers.Unconfined).use { catalog ->
                catalog.attach("test_db", Database.Config())
                catalog.detach("test_db")

                val ex = assertThrows<Conflict> {
                    catalog.attach("test_db", Database.Config())
                }
                assertEquals("xtdb/db-being-detached", ex.errCode())

                // Teardown finishes on the database's own dispatcher in the background; the conflict
                // is transient, so the name frees up and re-attach eventually succeeds.
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
        }
    }
}
