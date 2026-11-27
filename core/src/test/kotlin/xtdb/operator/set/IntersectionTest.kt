package xtdb.operator.set

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.test.AllocatorResolver
import java.time.ZoneOffset.UTC

@ExtendWith(AllocatorResolver::class)
class IntersectionTest {

    private val types = mapOf("a" to maybe(I64))

    private fun pages(vararg pages: List<Long?>) = pages.map { page -> page.map { mapOf("a" to it) } }

    private fun BufferAllocator.run(op: SetOperator.Matching, left: List<List<Map<String, *>>>, right: List<List<Map<String, *>>>) =
        op.openCursor(this, UTC, Long.MAX_VALUE, PagesCursor(this, types, left), PagesCursor(this, types, right))
            .use { it.consume() }
            .map { page -> page.map { it["a"] } }

    private fun BufferAllocator.intersect(left: List<List<Map<String, *>>>, right: List<List<Map<String, *>>>) =
        run(SetOperator.Intersect(types, types), left, right)

    private fun BufferAllocator.difference(left: List<List<Map<String, *>>>, right: List<List<Map<String, *>>>) =
        run(SetOperator.Difference(types, types), left, right)

    @Test
    fun `intersect keeps the left side's rows that the right side has, page by page`(al: BufferAllocator) {
        assertEquals(
            listOf(listOf(0L), listOf(100L)),
            al.intersect(pages(listOf(12, 0), listOf(100), listOf(7)), pages(listOf(10, 100), listOf(0)))
        )
    }

    @Test
    fun `intersect keeps the lesser of each row's cardinalities`(al: BufferAllocator) {
        assertEquals(
            listOf(1L, 1L, 2L, 2L),
            al.intersect(pages(listOf(1, 1, 1, 2, 2)), pages(listOf(1, 1, 2, 2, 2))).flatten()
        )
    }

    @Test
    fun `intersect with either side empty yields nothing`(al: BufferAllocator) {
        assertEquals(emptyList<Any>(), al.intersect(pages(), pages()))
        assertEquals(emptyList<Any>(), al.intersect(pages(emptyList()), pages(listOf(10, 15))))
        assertEquals(emptyList<Any>(), al.intersect(pages(listOf(10, 15)), pages()))
        assertEquals(emptyList<Any>(), al.intersect(pages(listOf(10)), pages(listOf(20))))
    }

    @Test
    fun `intersect matches null keys, keeping the lesser cardinality`(al: BufferAllocator) {
        assertEquals(
            listOf(null, null),
            al.intersect(pages(listOf(null, null, null, 2)), pages(listOf(null, null, 3))).flatten()
        )
    }

    @Test
    fun `difference keeps the left side's rows that the right side lacks, page by page`(al: BufferAllocator) {
        assertEquals(
            listOf(listOf(12L), listOf(100L)),
            al.difference(pages(listOf(12, 0), listOf(100), listOf(10)), pages(listOf(10, 15), listOf(0)))
        )
    }

    @Test
    fun `difference subtracts the right side's cardinality from the left's`(al: BufferAllocator) {
        assertEquals(
            listOf(1L),
            al.difference(pages(listOf(1, 1, 1, 2, 2)), pages(listOf(1, 1, 2, 2, 2))).flatten()
        )
    }

    @Test
    fun `difference matches null keys, subtracting the right side's cardinality`(al: BufferAllocator) {
        assertEquals(
            listOf(null, 2L),
            al.difference(pages(listOf(null, null, null, 2)), pages(listOf(null, null, 3))).flatten()
        )
    }

    @Test
    fun `difference counts the right side's first row`(al: BufferAllocator) {
        assertEquals(listOf(listOf(2L)), al.difference(pages(listOf(1, 2)), pages(listOf(1))))
    }

    @Test
    fun `difference with an empty side`(al: BufferAllocator) {
        assertEquals(emptyList<Any>(), al.difference(pages(), pages()))
        assertEquals(emptyList<Any>(), al.difference(pages(emptyList()), pages(listOf(10, 15))))
        assertEquals(listOf(listOf(10L, 15L)), al.difference(pages(listOf(10, 15)), pages(emptyList())))
    }
}
