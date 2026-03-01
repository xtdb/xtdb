package xtdb.api.log

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import xtdb.api.log.Log.Companion.tailAll
import xtdb.api.log.Log.Record
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

class LocalLogTest {

    @TempDir
    lateinit var tempDir: Path

    @Tag("integration")
    @RepeatedTest(5000)
    fun `close should cancel all subscription coroutines without leaking`() = runTest(timeout = 10.seconds) {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap())

        // Create a subscription
        val receivedRecords = mutableListOf<Record<SourceMessage>>()
        val subscription = log.tailAll(
            -1L,
            object : Log.RecordProcessor<SourceMessage> {
                override fun processRecords(records: List<Record<SourceMessage>>) {
                    records.forEach { receivedRecords.add(it) }
                }
            }
        )

        log.appendMessage(SourceMessage.FlushBlock(1))

        subscription.close()

        log.close()
    }

    private fun txMessage(id: Byte) = SourceMessage.Tx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage returns null when log is empty`() {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap())
        log.use {
            assertNull(log.readLastMessage())
        }
    }

    @Test
    fun `readLastMessage returns the message after appending one`() {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap())
        log.use {
            log.appendMessage(txMessage(1)).get()

            val lastMessage = log.readLastMessage()
            assertNotNull(lastMessage)
            assertTrue(lastMessage is SourceMessage.Tx)
            assertArrayEquals(byteArrayOf(-1, 1), (lastMessage as SourceMessage.Tx).payload)
        }
    }

    @Test
    fun `readLastMessage returns the last message after appending multiple`() {
        val log = LocalLog.Factory(tempDir.resolve("log")).openSourceLog(emptyMap())
        log.use {
            log.appendMessage(txMessage(1)).get()
            log.appendMessage(txMessage(2)).get()
            log.appendMessage(txMessage(3)).get()

            val lastMessage = log.readLastMessage()
            assertNotNull(lastMessage)
            assertTrue(lastMessage is SourceMessage.Tx)
            assertArrayEquals(byteArrayOf(-1, 3), (lastMessage as SourceMessage.Tx).payload)
        }
    }
}