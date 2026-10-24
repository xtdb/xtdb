package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class VectorTest {

    @Test
    fun `doesn't promote if source is a null-vec #4675`(al: BufferAllocator) {
        LongVector(al, "i64", true).use { vec ->
            val maybePromoted = vec.maybePromote(al, NULL_TYPE, true)
            assertSame(vec, maybePromoted)
        }
    }

    @Test
    fun `a renamed vector's field and its relation's schema carry the new name`(al: BufferAllocator) {
        LongVector(al, "a", false).use { vec ->
            vec.writeLong(1)
            val renamed = vec.withName("b")

            assertEquals("b", renamed.field.name)
            assertEquals(listOf("b"), RelationReader.from(listOf(renamed)).schema.fields.map { it.name })
        }
    }
}
