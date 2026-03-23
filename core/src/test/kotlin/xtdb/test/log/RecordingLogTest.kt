package xtdb.test.log

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.SourceMessage

class RecordingLogTest {

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage returns null when log is empty`() {
        val log = RecordingLog.Factory().openSourceLog(emptyMap())
        assertNull(log.readLastMessage())
    }

    @Test
    fun `readLastMessage returns the message after appending one`() = runTest {
        val log = RecordingLog.Factory().openSourceLog(emptyMap())

        log.appendMessage(txMessage(1))

        val lastMessage = log.readLastMessage()
        assertNotNull(lastMessage)
        assertTrue(lastMessage is SourceMessage.LegacyTx)
        assertArrayEquals(byteArrayOf(-1, 1), (lastMessage as SourceMessage.LegacyTx).payload)
    }

    @Test
    fun `readLastMessage returns the last message after appending multiple`() = runTest {
        val log = RecordingLog.Factory().openSourceLog(emptyMap())

        log.appendMessage(txMessage(1))
        log.appendMessage(txMessage(2))
        log.appendMessage(txMessage(3))

        val lastMessage = log.readLastMessage()
        assertNotNull(lastMessage)
        assertTrue(lastMessage is SourceMessage.LegacyTx)
        assertArrayEquals(byteArrayOf(-1, 3), (lastMessage as SourceMessage.LegacyTx).payload)
    }

    @Test
    fun `factory can be initialized with messages`() {
        val log = RecordingLog.Factory()
            .messages(listOf(txMessage(1), txMessage(2), txMessage(3)))
            .openSourceLog(emptyMap())

        val lastMessage = log.readLastMessage()
        assertNotNull(lastMessage)
        assertTrue(lastMessage is SourceMessage.LegacyTx)
        assertArrayEquals(byteArrayOf(-1, 3), (lastMessage as SourceMessage.LegacyTx).payload)
    }
}
