package xtdb.operator.set

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.error.Incorrect
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class UnionAllTest {

    private val ab = mapOf("a" to I64, "b" to I64)

    private fun BufferAllocator.unionAll(left: List<List<Map<String, *>>>, right: List<List<Map<String, *>>>) =
        SetOperator.UnionAll(ab, ab)
            .openCursor(PagesCursor(this, ab, left), PagesCursor(this, ab, right))
            .use { it.consume() }

    @Test
    fun `emits the left side's pages, then the right side's`(al: BufferAllocator) {
        assertEquals(
            listOf(
                listOf(mapOf("a" to 12L, "b" to 10L), mapOf("a" to 0L, "b" to 15L)),
                listOf(mapOf("a" to 100L, "b" to 15L)),
                listOf(mapOf("a" to 10L, "b" to 1L), mapOf("a" to 15L, "b" to 2L)),
                listOf(mapOf("a" to 83L, "b" to 3L)),
            ),
            al.unionAll(
                listOf(
                    listOf(mapOf("a" to 12L, "b" to 10L), mapOf("a" to 0L, "b" to 15L)),
                    listOf(mapOf("a" to 100L, "b" to 15L)),
                ),
                listOf(
                    listOf(mapOf("a" to 10L, "b" to 1L), mapOf("a" to 15L, "b" to 2L)),
                    listOf(mapOf("a" to 83L, "b" to 3L)),
                ),
            )
        )
    }

    @Test
    fun `skips empty pages on either side`(al: BufferAllocator) {
        assertEquals(
            listOf(listOf(mapOf("a" to 10L, "b" to 1L)), listOf(mapOf("a" to 15L, "b" to 2L))),
            al.unionAll(
                listOf(emptyList(), listOf(mapOf("a" to 10L, "b" to 1L))),
                listOf(emptyList(), emptyList(), listOf(mapOf("a" to 15L, "b" to 2L))),
            )
        )
    }

    @Test
    fun `no pages on either side yields nothing`(al: BufferAllocator) {
        assertEquals(emptyList<Any>(), al.unionAll(emptyList(), emptyList()))
        assertEquals(emptyList<Any>(), al.unionAll(listOf(emptyList()), listOf(emptyList())))
    }

    @Test
    fun `output types merge both sides' types`() {
        assertEquals(
            mapOf("a" to maybe(I64), "b" to I64),
            SetOperator.UnionAll(mapOf("a" to I64, "b" to I64), mapOf("a" to maybe(I64), "b" to I64)).vecTypes
        )
    }

    @Test
    fun `sides with different columns are incompatible`() {
        assertThrows<Incorrect> { SetOperator.UnionAll(mapOf("a" to I64), mapOf("b" to I64)) }
        assertThrows<Incorrect> { SetOperator.UnionAll(mapOf("a" to I64), mapOf("a" to UTF8, "b" to I64)) }
    }
}
