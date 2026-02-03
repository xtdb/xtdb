package xtdb.api.log

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.Log.*
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.seconds

class InMemoryLogSubscribeTest {

    private fun txMessage(id: Byte) = Message.Tx(byteArrayOf(-1, id))

    @Test
    fun `assignment callback fires immediately`() = runTest(timeout = 10.seconds) {
        val assignedPartitions = AtomicReference<Collection<Int>>(null)

        val subscriber = mockk<Subscriber> {
            every { processRecords(any()) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        val listener = object : AssignmentListener {
            override fun onPartitionsAssigned(partitions: Collection<Int>): Map<Int, LogOffset> {
                assignedPartitions.set(partitions)
                return emptyMap()
            }
            override fun onPartitionsRevoked(partitions: Collection<Int>) {}
        }

        InMemoryLog.Factory().openLog(emptyMap()).use { log ->
            log.subscribe(subscriber, listener).use {
                // Assignment should be immediate (synchronous)
                assertEquals(listOf(0), assignedPartitions.get()?.toList())
            }
        }
    }

    @Test
    fun `returned offset determines start position`() = runTest(timeout = 10.seconds) {
        val receivedRecords = Collections.synchronizedList(mutableListOf<Record>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(any()) } answers {
                receivedRecords.addAll(firstArg())
            }
            every { latestProcessedMsgId } returns -1
        }

        InMemoryLog.Factory().openLog(emptyMap()).use { log ->
            // Append some messages first
            log.appendMessage(txMessage(0)).get()
            log.appendMessage(txMessage(1)).get()
            log.appendMessage(txMessage(2)).get()

            // Subscribe starting at offset 2 (skip first two messages)
            val listener = object : AssignmentListener {
                override fun onPartitionsAssigned(partitions: Collection<Int>): Map<Int, LogOffset> {
                    return mapOf(0 to 2L)
                }
                override fun onPartitionsRevoked(partitions: Collection<Int>) {}
            }

            log.subscribe(subscriber, listener).use {
                while (synchronized(receivedRecords) { receivedRecords.size } < 1) delay(50)
            }
        }

        synchronized(receivedRecords) {
            assertTrue(receivedRecords.isNotEmpty())
            val firstRecord = receivedRecords.first()
            assertEquals(2L, firstRecord.logOffset)
        }
    }

    @Test
    fun `revocation callback fires on close`() {
        val revokedPartitions = AtomicReference<Collection<Int>>(null)

        val subscriber = mockk<Subscriber> {
            every { processRecords(any()) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        val listener = object : AssignmentListener {
            override fun onPartitionsAssigned(partitions: Collection<Int>): Map<Int, LogOffset> {
                return emptyMap()
            }
            override fun onPartitionsRevoked(partitions: Collection<Int>) {
                revokedPartitions.set(partitions)
            }
        }

        InMemoryLog.Factory().openLog(emptyMap()).use { log ->
            log.subscribe(subscriber, listener).close()
        }

        assertEquals(listOf(0), revokedPartitions.get()?.toList())
    }
}
