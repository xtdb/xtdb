package xtdb.trie

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.RecencyLevel.*
import java.time.Instant

class RecencyLevelTest {

    private fun schedule(pinnedDepth: Int): List<String> {
        val steps = mutableListOf<String>()
        var depth = 0
        var levelsUsed = 0

        while (depth < 6) {
            val level = RecencyLevel.scheduled(depth, levelsUsed, pinnedDepth)
            if (level != null) {
                steps += level.name
                levelsUsed++
            } else {
                steps += "IID"
                depth++
            }
        }

        return steps
    }

    @Test
    fun `IID and recency levels interleave, IID first`() {
        assertEquals(
            listOf("IID", "DAY", "IID", "SIX_HOURS", "IID", "HOUR", "IID", "IID", "IID"),
            schedule(pinnedDepth = 0)
        )
    }

    @Test
    fun `recency levels catch up below the IID levels a file's part pins`() {
        assertEquals(
            listOf("IID", "IID", "DAY", "SIX_HOURS", "IID", "HOUR", "IID", "IID", "IID"),
            schedule(pinnedDepth = 2)
        )
    }

    @Test
    fun `a bucket is keyed by its inclusive upper bound`() {
        fun micros(s: String) = Instant.parse(s).asMicros

        assertEquals(micros("2020-01-02T00:00:00Z"), DAY.bucketFor(micros("2020-01-01T00:00:01Z")))
        assertEquals(micros("2020-01-01T00:00:00Z"), DAY.bucketFor(micros("2020-01-01T00:00:00Z")))
        assertEquals(micros("2020-01-01T12:00:00Z"), SIX_HOURS.bucketFor(micros("2020-01-01T07:30:00Z")))
        assertEquals(micros("2020-01-01T08:00:00Z"), HOUR.bucketFor(micros("2020-01-01T07:30:00Z")))
    }
}
