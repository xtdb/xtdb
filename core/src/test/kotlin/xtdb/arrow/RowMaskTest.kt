package xtdb.arrow

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal fun rowMask(rowCount: Int, set: (Int) -> Boolean): RowMask {
    val words = LongArray((rowCount + 63) ushr 6)
    for (row in 0 until rowCount)
        if (set(row)) words[row ushr 6] = words[row ushr 6] or (1L shl (row and 63))
    return RowMask.ofWords(words, rowCount)
}

class RowMaskTest {

    private fun maskOf(bits: List<Boolean>) = rowMask(bits.size) { bits[it] }

    @Test
    fun `selects exactly the set rows, in order`() {
        val mask = maskOf(listOf(true, false, true, true, false))

        mask.rowCount shouldBe 5
        mask.cardinality shouldBe 3
        mask.toSelection().toList() shouldBe listOf(0, 2, 3)
    }

    @Test
    fun `empty mask selects nothing`() {
        val mask = RowMask.ofWords(LongArray(0), 0)

        mask.rowCount shouldBe 0
        mask.wordCount shouldBe 0
        mask.cardinality shouldBe 0
        mask.toSelection().toList() shouldBe emptyList()
    }

    @Test
    fun `none set across several words`() {
        val mask = rowMask(130) { false }

        mask.wordCount shouldBe 3
        mask.cardinality shouldBe 0
        mask.toSelection().toList() shouldBe emptyList()
    }

    @Test
    fun `all set, ending in a partial word`() {
        val mask = rowMask(130) { true }

        mask.cardinality shouldBe 130
        mask.toSelection().toList() shouldBe (0 until 130).toList()
    }

    @Test
    fun `exactly one full word`() {
        rowMask(64) { true }.toSelection().toList() shouldBe (0 until 64).toList()
        rowMask(64) { it == 63 }.toSelection().toList() shouldBe listOf(63)
        RowMask.ofWords(longArrayOf(-1L), 64).cardinality shouldBe 64
    }

    @Test
    fun `rows either side of each word boundary`() {
        val setRows = setOf(0, 62, 63, 64, 65, 127, 128, 191, 199)
        val mask = rowMask(200) { it in setRows }

        mask.cardinality shouldBe setRows.size
        mask.toSelection().toList() shouldBe setRows.sorted()
        mask.word(0) shouldBe (1L or (1L shl 62) or (1L shl 63))
        mask.word(1) shouldBe (1L or 2L or (1L shl 63))
    }

    @Test
    fun `ofWords rejects a set bit at or past rowCount`() {
        shouldThrow<IllegalArgumentException> { RowMask.ofWords(longArrayOf(1L shl 5), 5) }
        shouldThrow<IllegalArgumentException> { RowMask.ofWords(longArrayOf(0L, 1L shl 63), 70) }

        RowMask.ofWords(longArrayOf(0L, 1L shl 5), 70).toSelection().toList() shouldBe listOf(69)
    }

    @Test
    fun `ofWords rejects a word count that doesn't cover rowCount exactly`() {
        shouldThrow<IllegalArgumentException> { RowMask.ofWords(LongArray(1), 65) }
        shouldThrow<IllegalArgumentException> { RowMask.ofWords(LongArray(2), 64) }
        shouldThrow<IllegalArgumentException> { RowMask.ofWords(LongArray(1), 0) }
        shouldThrow<IllegalArgumentException> { RowMask.ofWords(LongArray(0), -1) }
    }
}
