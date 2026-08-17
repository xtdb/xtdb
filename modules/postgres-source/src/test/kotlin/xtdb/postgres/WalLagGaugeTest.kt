package xtdb.postgres

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WalLagGaugeTest {

    private class StubDriver(private val lagBytes: () -> Long?) : PostgresDriver {
        override fun openSnapshot(): PostgresDriver.SnapshotReader = error("unused")
        override suspend fun openStream(startLsn: Long): PostgresDriver.ChangeStream = error("unused")
        override fun publicationExists() = true
        override fun queryWalLagBytes() = lagBytes()
        override fun close() = Unit
    }

    private fun SimpleMeterRegistry.walLag() =
        get("xtdb.postgres_source.wal_lag_bytes").gauge().value()

    private fun openSource(reg: SimpleMeterRegistry, lagBytes: () -> Long?) =
        PostgresSource("xtdb", StubDriver(lagBytes), "test_slot", DirectMirror(), reg)

    @Test
    fun `NaN until the slot is readable, and again once it isn't`() {
        val reg = SimpleMeterRegistry()
        var lag: Long? = null

        openSource(reg) { lag }.use {
            assertTrue(reg.walLag().isNaN(), "no reading yet")

            lag = 0
            assertEquals(0.0, reg.walLag(), "0 is caught-up, once we've read it")

            lag = 8192
            assertEquals(8192.0, reg.walLag())

            lag = null
            assertTrue(reg.walLag().isNaN(), "slot gone — unknown, not caught up")
        }
    }

    @Test
    fun `NaN when the query throws`() {
        val reg = SimpleMeterRegistry()
        var refuseConnection = false

        openSource(reg) { if (refuseConnection) throw IllegalStateException("connection refused") else 8192L }.use {
            assertEquals(8192.0, reg.walLag())

            refuseConnection = true
            assertTrue(reg.walLag().isNaN(), "failed read — unknown, not caught up")
        }
    }
}
