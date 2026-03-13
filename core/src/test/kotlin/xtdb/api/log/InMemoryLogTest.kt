package xtdb.api.log

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class InMemoryLogTest {

    private fun txMessage(id: Byte) = SourceMessage.Tx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage always returns null`() {
        val log = InMemoryLog.Factory().openSourceLog(emptyMap())
        log.use {
            assertNull(log.readLastMessage())

            log.appendMessage(txMessage(1)).get()

            // Still null because InMemoryLog has no persistence
            assertNull(log.readLastMessage())
        }
    }
}
