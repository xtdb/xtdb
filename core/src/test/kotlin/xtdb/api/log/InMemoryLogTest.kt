package xtdb.api.log

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class InMemoryLogTest {

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage always returns null`() = runTest {
        val log = InMemoryLog.Factory().openSourceLog(emptyMap())
        log.use {
            assertNull(log.readLastMessage())

            log.appendMessage(txMessage(1))

            // Still null because InMemoryLog has no persistence
            assertNull(log.readLastMessage())
        }
    }
}
