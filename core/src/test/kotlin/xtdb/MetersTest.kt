package xtdb

import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicLong

class MetersTest {

    private fun Meters.gauge(name: String, source: AtomicLong) =
        register { reg -> Gauge.builder(name, source) { it.get().toDouble() }.register(reg) }

    private fun SimpleMeterRegistry.names() = meters.map { it.id.name }

    @Test
    fun `a gauge reads its own registrant, not one an earlier owner left behind`() {
        val reg = SimpleMeterRegistry()
        val abandoned = AtomicLong(1)
        val live = AtomicLong(2)

        Meters(reg).gauge("test.gauge", abandoned)

        Meters(reg).use { meters ->
            meters.gauge("test.gauge", live)
            assertEquals(2.0, reg.get("test.gauge").gauge().value(), "the live owner's source")
        }

        assertEquals(emptyList<String>(), reg.names(), "and the evicting owner still cleans up after itself")
    }

    @Test
    fun `closing takes the percentiles a summary publishes alongside it`() {
        val reg = SimpleMeterRegistry()

        Meters(reg).use { meters ->
            meters.register { r ->
                DistributionSummary.builder("test.summary").publishPercentiles(0.5, 0.99).register(r)
            }

            assertTrue(reg.names().size > 1, "the summary publishes gauges of its own, was: ${reg.names()}")
        }

        assertEquals(emptyList<String>(), reg.names())
    }

    @Test
    fun `a node without metrics builds nothing`() {
        var built = false

        Meters(null).use { meters ->
            assertNull(meters.register { reg -> built = true; Gauge.builder("test.gauge") { 1.0 }.register(reg) })
        }

        assertEquals(false, built)
    }
}
