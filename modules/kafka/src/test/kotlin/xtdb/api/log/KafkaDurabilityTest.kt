package xtdb.api.log

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class KafkaDurabilityTest {

    @Test
    fun `an operator property cannot weaken acks`() {
        assertEquals("all", mapOf("acks" to "1").producerConfig()["acks"])
    }

    @Test
    fun `every other producer default stays overridable`() {
        val overridden = mapOf(
            "linger.ms" to "50",
            "enable.idempotence" to "false",
            "compression.type" to "lz4",
        )

        assertEquals(
            overridden, overridden.producerConfig() - "acks",
            "equality rather than key-by-key, so that pinning a second key fails here too"
        )
    }

    @Test
    fun `an operator property that is not a default is carried through`() {
        assertEquals("SASL_SSL", mapOf("security.protocol" to "SASL_SSL").producerConfig()["security.protocol"])
    }
}
