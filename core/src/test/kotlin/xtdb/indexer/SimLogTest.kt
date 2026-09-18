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
    fun `an enqueued record is not readable until its handle completes`() = runTest(timeout = 5.seconds) {
        coroutineScope {
            SimLog<String>("test", rand).use { log ->
                val handle = log.enqueueMessage("one")

                assertEquals(-1L, log.latestSubmittedOffset())
                assertEquals(emptyList<String>(), log.topic.map { it.message })

                assertEquals(0L, handle.await().logOffset)

                assertEquals(0L, log.latestSubmittedOffset())
                assertEquals(listOf("one"), log.topic.map { it.message })
            }
        }
    }

    @Test
    fun `records land in the order their enqueues fixed`() = runTest(timeout = 5.seconds) {
        coroutineScope {
            SimLog<String>("test", rand).use { log ->
                val handles = (1..5).map { log.enqueueMessage("msg-$it") }

                assertEquals(emptyList<String>(), log.topic.map { it.message })

                handles.forEachIndexed { idx, handle -> assertEquals(idx.toLong(), handle.await().logOffset) }

                assertEquals((1..5).map { "msg-$it" }, log.topic.map { it.message })
            }
        }
    }

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
