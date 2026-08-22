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

                    launch { log.tailAll(partition = 0, afterMsgId = -1) { _ -> error("plainConsumer failure") } }

                    log.appendMessage("trigger")
                }
            }
        }

        assertEquals("plainConsumer failure", ex.message)
    }
}
