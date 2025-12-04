package xtdb.test.log

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.Log.Message

class RecordingLogTest {

    private fun txMessage(id: Byte) = Message.Tx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage returns null when log is empty`() {
        val log = RecordingLog.Factory().openLog(emptyMap())
        assertNull(log.readLastMessage())
    }

    @Test
    fun `readLastMessage returns the message after appending one`() {
        val log = RecordingLog.Factory().openLog(emptyMap())

        log.appendMessage(txMessage(1)).get()

        val lastMessage = log.readLastMessage()
        assertNotNull(lastMessage)
        assertTrue(lastMessage is Message.Tx)
        assertArrayEquals(byteArrayOf(-1, 1), (lastMessage as Message.Tx).payload)
    }

    @Test
    fun `readLastMessage returns the last message after appending multiple`() {
        val log = RecordingLog.Factory().openLog(emptyMap())

        log.appendMessage(txMessage(1)).get()
        log.appendMessage(txMessage(2)).get()
        log.appendMessage(txMessage(3)).get()

        val lastMessage = log.readLastMessage()
        assertNotNull(lastMessage)
        assertTrue(lastMessage is Message.Tx)
        assertArrayEquals(byteArrayOf(-1, 3), (lastMessage as Message.Tx).payload)
    }

    @Test
    fun `factory can be initialized with messages`() {
        val log = RecordingLog.Factory()
            .messages(listOf(txMessage(1), txMessage(2), txMessage(3)))
            .openLog(emptyMap())

        val lastMessage = log.readLastMessage()
        assertNotNull(lastMessage)
        assertTrue(lastMessage is Message.Tx)
        assertArrayEquals(byteArrayOf(-1, 3), (lastMessage as Message.Tx).payload)
    }
}
