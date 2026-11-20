package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class RelationAsStructReaderTest {

    private fun openDocs(al: BufferAllocator) =
        RelationAsStructReader("docs", Relation.openFromRows(al, List(2) { mapOf("fooBar" to it.toLong()) }))

    @Test
    fun `a row keeps its columns' stored names`(al: BufferAllocator) {
        openDocs(al).use { rdr ->
            assertEquals(mapOf("fooBar" to 1L), rdr.getObject(1))
            assertEquals(listOf(mapOf("fooBar" to 0L), mapOf("fooBar" to 1L)), rdr.asList)
        }
    }

    @Test
    fun `a singleton list of rows keeps their columns' stored names`(al: BufferAllocator) {
        SingletonListReader("col", openDocs(al)).use { rdr ->
            assertEquals(listOf(mapOf("fooBar" to 0L), mapOf("fooBar" to 1L)), leaves(rdr.getObject(0)))
            assertEquals(listOf(mapOf("fooBar" to 0L), mapOf("fooBar" to 1L)), leaves(rdr.asList))
        }
    }

    private fun leaves(v: Any?): List<Any?> = if (v is List<*>) v.flatMap(::leaves) else listOf(v)
}
