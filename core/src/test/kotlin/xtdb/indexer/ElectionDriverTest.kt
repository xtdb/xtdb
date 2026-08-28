package xtdb.indexer

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class ElectionDriverTest {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `the assert clause arms at the assert interval`() = runTest {
        val driver = RealElectionDriver(assertInterval = 1.seconds)
        val nothingQueued = Channel<Unit>()
        val clock = testScheduler
        val start = clock.currentTime

        val firedAt = select {
            driver.run { onAssertTimeout { clock.currentTime } }
            nothingQueued.onReceive { -1L }
        }

        assertEquals(
            1_000L, firedAt - start,
            "an idle leader asserts once the interval is up, and a leader with traffic never gets there"
        )
    }
}
