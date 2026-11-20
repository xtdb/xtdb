package xtdb.api

import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.ICursor.Companion.wrapTracing
import xtdb.arrow.RelationReader
import java.time.Duration
import java.util.function.Consumer

class TracingCursorTest {

    private open class FakeScan(private var pages: Int) : ICursor {
        override val cursorType get() = "fake-scan"
        override val childCursors get() = emptyList<ICursor>()

        override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
            if (pages == 0) return false
            pages--
            c.accept(RelationReader.from(emptyList(), 3))
            return true
        }
    }

    @Test
    fun `total_time excludes the consumer's accept`() {
        val consumerTime = Duration.ofMillis(20)
        RootAllocator().use { root ->
            FakeScan(pages = 3).wrapTracing(null, null, null, null, root.newChildAllocator("op", 0, Long.MAX_VALUE)).use { cursor ->
                cursor.forEachRemaining { Thread.sleep(consumerTime.toMillis()) }

                val ea = cursor.explainAnalyze!!
                assertEquals(3, ea.pageCount)
                assertEquals(9L, ea.rowCount)
                assertTrue(ea.totalTime < consumerTime, "total_time ${ea.totalTime} swallowed the consumer's 3 x $consumerTime")
            }
        }
    }

    @Test
    fun `a failing inner close is what surfaces, with the operator allocator's leak suppressed`() {
        val innerFailure = IllegalStateException("inner close failed")
        val allocator = RootAllocator().newChildAllocator("op", 0, Long.MAX_VALUE)
        allocator.buffer(64)

        val cursor = object : FakeScan(pages = 0) {
            override fun close() = throw innerFailure
        }.wrapTracing(null, null, null, null, allocator)

        val thrown = assertThrows<IllegalStateException> { cursor.close() }
        assertSame(innerFailure, thrown)
        assertTrue(thrown.suppressed.any { it.message.orEmpty().startsWith("Memory was leaked") })
    }
}
