package xtdb.database

import clojure.lang.Keyword
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase
import xtdb.error.Conflict

class DatabaseCatalogTest {

    private val ERROR_CODE = Keyword.intern("xtdb.error", "code")

    private fun Conflict.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")

    @Test
    fun `reattach during detach returns transient conflict (#5613)`() {
        val scheduler = TestCoroutineScheduler()
        val dispatcher = StandardTestDispatcher(scheduler)

        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base, dispatcher).use { catalog ->
                catalog.attach("test_db", Database.Config())
                // close is enqueued on the paused dispatcher
                catalog.detach("test_db")

                val ex = assertThrows<Conflict> {
                    catalog.attach("test_db", Database.Config())
                }
                assertEquals("xtdb/db-being-detached", ex.errCode())

                // drain the closer, then re-attach succeeds
                scheduler.advanceUntilIdle()
                catalog.attach("test_db", Database.Config())
            }
        }
    }
}
