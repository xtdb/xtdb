package xtdb.indexer

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.SimulationTestBase
import kotlin.time.Duration.Companion.seconds

class SimLogTest : SimulationTestBase() {

    @Test
    fun `consumer processRecords failure propagates via the parent scope`() = runTest(timeout = 5.seconds) {
        val ex = assertThrows<IllegalStateException> {
            coroutineScope {
                SimLog<String>("test", rand).use { log ->
                    launch { log.tailAll(partition = 0, afterMsgId = -1) { _ -> error("consumer failure") } }

                    log.appendMessage("trigger")
                }
            }
        }

        assertEquals("consumer failure", ex.message)
    }

}
