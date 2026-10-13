package xtdb.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class IOUtilTest {
    @Test
    fun `closeAll closes every element when some throw, surfacing the first failure with the rest suppressed`() {
        val closed = mutableListOf<Int>()
        val first = IllegalStateException("first")
        val second = IllegalStateException("second")

        val closeables = listOf(
            AutoCloseable { closed += 0 },
            AutoCloseable { closed += 1; throw first },
            null,
            AutoCloseable { closed += 3; throw second },
            AutoCloseable { closed += 4 },
        )

        val thrown = assertThrows<IllegalStateException> { closeables.closeAll() }

        assertEquals(listOf(0, 1, 3, 4), closed)
        assertEquals(first, thrown)
        assertEquals(listOf(second), thrown.suppressed.toList())
    }
}
