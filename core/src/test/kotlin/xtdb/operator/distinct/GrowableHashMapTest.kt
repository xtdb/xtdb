package xtdb.operator.distinct

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.error.Unsupported
import xtdb.test.AllocatorResolver
import kotlin.random.Random

@ExtendWith(AllocatorResolver::class)
class GrowableHashMapTest {

    private fun GrowableHashMap.dedupe(keys: List<Int>, hashFn: (Int) -> Int): List<Int> {
        val rows = mutableListOf<Int>()
        return keys.map { key ->
            val newRowIdx = rows.size
            findOrInsert(hashFn(key), newRowIdx, { rowIdx -> if (rows[rowIdx] == key) 1 else 0 }, { rows.add(key) })
        }
    }

    private fun firstOccurrences(keys: List<Int>): List<Int> {
        val firstIdxs = mutableMapOf<Int, Int>()
        return keys.map { key -> firstIdxs.getOrPut(key) { firstIdxs.size } }
    }

    @Test
    fun `each key maps to the row of its first occurrence, across several growths`(al: BufferAllocator) {
        val keys = Random(0).let { rnd -> List(20_000) { rnd.nextInt(5_000) } }

        GrowableHashMap(al).use { map ->
            assertEquals(firstOccurrences(keys), map.dedupe(keys) { it * -1640531527 })
        }
    }

    @Test
    fun `rows under one hash are told apart by the comparator`(al: BufferAllocator) {
        val keys = (0 until 200).toList() + (199 downTo 0)

        GrowableHashMap(al).use { map ->
            assertEquals(firstOccurrences(keys), map.dedupe(keys) { 42 })
        }
    }

    @Test
    fun `rows sharing a home slot are found along its probe sequence`(al: BufferAllocator) {
        val keys = (0 until 200).toList() + (0 until 200)

        GrowableHashMap(al).use { map ->
            assertEquals(firstOccurrences(keys), map.dedupe(keys) { it shl 16 })
        }
    }

    @Test
    fun `only a comparator result of 1 matches an existing row`(al: BufferAllocator) {
        GrowableHashMap(al).use { map ->
            assertEquals(0, map.findOrInsert(7, 0, { error("empty map") }, {}))
            assertEquals(1, map.findOrInsert(7, 1, { -1 }, {}))
            assertEquals(2, map.findOrInsert(7, 2, { 0 }, {}))
            assertEquals(1, map.findOrInsert(7, 3, { rowIdx -> if (rowIdx == 1) 1 else 0 }, {}))
        }
    }

    @Test
    fun `a row whose insert throws is never compared against`(al: BufferAllocator) {
        GrowableHashMap(al).use { map ->
            assertThrows<IllegalStateException> { map.findOrInsert(7, 0, { error("empty map") }, { error("copy failed") }) }
            assertEquals(0, map.findOrInsert(7, 0, { error("row 0 was never inserted") }, {}))
        }
    }

    @Test
    fun `growing past the maximum capacity fails rather than wrapping`(al: BufferAllocator) {
        GrowableHashMap(al, maxCapacity = 32).use { map ->
            val keys = (0 until 19).toList()
            assertEquals(firstOccurrences(keys), map.dedupe(keys) { it })

            assertThrows<Unsupported> { map.dedupe(listOf(19)) { it } }
        }
    }
}
