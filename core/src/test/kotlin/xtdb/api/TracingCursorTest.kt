package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.api.ICursor.Companion.wrapTracing
import xtdb.arrow.RelationReader
import java.time.Duration
import java.util.function.Consumer

class TracingCursorTest {

    private class FakeScan(private var pages: Int) : ICursor {
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
        val cursor = FakeScan(pages = 3).wrapTracing(null, null)

        cursor.forEachRemaining { Thread.sleep(consumerTime.toMillis()) }

        val ea = cursor.explainAnalyze!!
        assertEquals(3, ea.pageCount)
        assertEquals(9L, ea.rowCount)
        assertTrue(ea.totalTime < consumerTime, "total_time ${ea.totalTime} swallowed the consumer's 3 x $consumerTime")
    }
}
