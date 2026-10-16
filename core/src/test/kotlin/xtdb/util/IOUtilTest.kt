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

    @Test
    fun `useAll surfaces the body's exception with close failures suppressed`() {
        val closed = mutableListOf<Int>()
        val bodyError = IllegalStateException("body")
        val closeError = IllegalStateException("close")

        val closeables = listOf(
            AutoCloseable { closed += 0; throw closeError },
            AutoCloseable { closed += 1 },
        )

        val thrown = assertThrows<IllegalStateException> { closeables.useAll { throw bodyError } }

        assertEquals(listOf(0, 1), closed)
        assertEquals(bodyError, thrown)
        assertEquals(listOf(closeError), thrown.suppressed.toList())
    }

    private fun returnFromInside(closeables: List<AutoCloseable>): Int {
        closeables.useAll { return 1 }
    }

    @Test
    fun `useAll closes every element when the block returns from the enclosing function`() {
        val closed = mutableListOf<Int>()

        assertEquals(1, returnFromInside(listOf(AutoCloseable { closed += 0 }, AutoCloseable { closed += 1 })))
        assertEquals(listOf(0, 1), closed)
    }

    @Test
    fun `closeOnCatch surfaces the body's exception with the close failure suppressed`() {
        val bodyError = IllegalStateException("body")
        val closeError = IllegalStateException("close")

        val thrown = assertThrows<IllegalStateException> {
            AutoCloseable { throw closeError }.closeOnCatch { throw bodyError }
        }

        assertEquals(bodyError, thrown)
        assertEquals(listOf(closeError), thrown.suppressed.toList())
    }
}
