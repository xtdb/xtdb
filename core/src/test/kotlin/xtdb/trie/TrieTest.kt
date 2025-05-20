package xtdb.trie

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate

class TrieTest {
    @Test
    fun `test five-digit years`() {
        val key = Trie.Key(0, LocalDate.parse("+10001-01-01"), null, 0)

        assertEquals(
            "l00-r100010101-b00", key.toString(),
            "creates keys without plus signs"
        )

        assertEquals(
            key, Trie.parseKey("l00-r100010101-b00"),
            "handles without a sign"
        )

        assertEquals(
            key, Trie.parseKey("l00-r+100010101-b00"),
            "handles with a plus sign (for historical reasons)"
        )
    }

}