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
            val maybePromoted = vec.maybePromote(al, VectorType.NULL.fieldType)
            assertSame(vec, maybePromoted)
        }
    }
}