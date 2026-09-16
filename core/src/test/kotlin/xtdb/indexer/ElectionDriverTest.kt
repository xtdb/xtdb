package xtdb.indexer

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
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

    @Test
    fun `an election timeout outlasts a run of asserts, and is redrawn per call`() {
        val driver = RealElectionDriver(assertInterval = 100.milliseconds, random = Random(0))

        val draws = List(100) { driver.electionTimeout() }

        assertTrue(
            draws.all { it in 500.milliseconds..1.seconds },
            "an election outlasts a run of asserts whatever the interval is set to, was: $draws"
        )
        assertTrue(
            draws.distinct().size > 1,
            "two candidates converge on one of them by drawing apart, so the draw is per call"
        )
    }

}
