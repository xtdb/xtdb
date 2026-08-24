package xtdb.api.error

import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Anomaly.Companion.toAnomaly
import xtdb.api.error.Anomaly.Companion.wrapAnomaly
import java.nio.channels.ClosedByInterruptException

class AnomalyTest {

    @Test
    fun `an interrupt escapes wrapAnomaly unclassified`() {
        val interrupt = InterruptedException("stopping")

        assertSame(interrupt, assertThrows<InterruptedException> { wrapAnomaly<Unit> { throw interrupt } })
    }

    @Test
    fun `a closed-by-interrupt channel escapes wrapAnomaly as an interrupt`() {
        val closed = ClosedByInterruptException()

        assertSame(closed, assertThrows<InterruptedException> { wrapAnomaly<Unit> { throw closed } }.cause)
    }

    @Test
    fun `wrapAnomaly classifies everything else`() {
        assertThrows<Incorrect> { wrapAnomaly<Unit> { throw IllegalArgumentException("nope") } }
        assertThrows<Fault> { wrapAnomaly<Unit> { throw RuntimeException("boom") } }
    }

    @Test
    fun `toAnomaly gives an interrupt a category for the wire boundaries`() {
        assertInstanceOf(Interrupted::class.java, InterruptedException("stopping").toAnomaly())
        assertInstanceOf(Interrupted::class.java, ClosedByInterruptException().toAnomaly())
    }
}
