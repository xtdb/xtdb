package xtdb.operator

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.api.error.Fault
import xtdb.arrow.Relation
import xtdb.test.AllocatorResolver
import java.time.ZoneOffset

@ExtendWith(AllocatorResolver::class)
class OrderSpecTest {

    @Test
    fun `comparing rows by no order specs is a fault`(al: BufferAllocator) {
        Relation.openFromCols(al, mapOf("a" to listOf(1L, 2L))).use { rel ->
            val fault = assertThrows<Fault> { emptyList<OrderSpec>().comparator(rel, rel, ZoneOffset.UTC) }
            assertEquals(Keyword.intern("xtdb/no-order-specs"), fault.data.valAt(Keyword.intern("xtdb.error", "code")))
        }
    }
}
