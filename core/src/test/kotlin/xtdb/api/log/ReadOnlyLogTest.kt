package xtdb.api.log

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Incorrect
import xtdb.api.log.SourceMessage.FlushBlock
import java.time.InstantSource

class ReadOnlyLogTest {

    @Test
    fun `a read-only log refuses every write, and none reaches the log beneath it`() = runTest {
        val log = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val readOnly = ReadOnlyLog(log)

        assertThrows<Incorrect> { readOnly.enqueueMessage(FlushBlock(null)) }
        assertThrows<Incorrect> { readOnly.appendMessage(FlushBlock(null)) }
        assertThrows<Incorrect> { readOnly.appendMessageBlocking(FlushBlock(null)) }

        assertEquals(-1L, log.latestSubmittedOffset(0))
    }
}
