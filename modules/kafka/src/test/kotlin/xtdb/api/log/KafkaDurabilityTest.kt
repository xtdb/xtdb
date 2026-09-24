package xtdb.api.log

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class KafkaDurabilityTest {

    @Test
    fun `an operator property cannot weaken acks`() {
        assertEquals("all", mapOf("acks" to "1").producerConfig(pipelined = false)["acks"])
    }

    @Test
    fun `every other producer default stays overridable`() {
        val overridden = mapOf(
            "linger.ms" to "50",
            "enable.idempotence" to "false",
            "compression.type" to "lz4",
        )

        assertEquals(
            overridden, overridden.producerConfig(pipelined = false) - "acks",
            "equality rather than key-by-key, so that pinning a second key fails here too"
        )
    }

    @Test
    fun `an operator property that is not a default is carried through`() {
        assertEquals("SASL_SSL", mapOf("security.protocol" to "SASL_SSL").producerConfig(pipelined = false)["security.protocol"])
    }

    @Test
    fun `an awaiting producer does not linger`() {
        assertEquals("0", emptyMap<String, String>().producerConfig(pipelined = false)["linger.ms"])
    }

    @Test
    fun `a pipelined producer lingers for as long as Kafka does by default`() {
        assertNull(emptyMap<String, String>().producerConfig(pipelined = true)["linger.ms"])
    }
}
