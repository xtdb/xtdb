package xtdb.indexer

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.SimulationTestBase
import xtdb.indexer.SimLog.Companion.launchSimLog
import kotlin.time.Duration.Companion.seconds

class SimLogTest : SimulationTestBase() {

    @Test
    fun `plainConsumer processRecords failure propagates via the parent scope`() = runTest(timeout = 5.seconds) {
        val ex = assertThrows<IllegalStateException> {
            coroutineScope {
                SimLog<String>("test", rand).use { log ->
                    launchSimLog(log)

                    // Guarded on a non-empty batch, because the behaviour under test is that a failure
                    // while *delivering a record* propagates — unguarded, the first empty tick satisfies it.
                    launch {
                        log.tailAll(partition = 0, afterMsgId = -1) { records ->
                            if (records.isNotEmpty()) error("plainConsumer failure")
                        }
                    }

                    log.appendMessage("trigger")
                }
            }
        }

        assertEquals("plainConsumer failure", ex.message)
    }
}
