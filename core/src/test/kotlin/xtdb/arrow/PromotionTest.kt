package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import xtdb.types.Fields
import xtdb.util.closeOnCatch

@ExtendWith(AllocatorResolver::class)
class PromotionTest {
    @Test
    fun `no-op if the type matches`(al: BufferAllocator) {
        Vector.fromField(al, Fields.I64.toArrowField("i64"))
            .use { v -> assertEquals(v, v.maybePromote(al, Fields.I64.fieldType)) }

        Vector.fromField(al, Fields.Struct("a" to Fields.I64, "b" to Fields.UTF8).toArrowField("struct"))
            .use { v -> assertEquals(v, v.maybePromote(al, Fields.Struct().fieldType)) }
    }

    @Test
    fun `adds nullable, but otherwise same vec`(al: BufferAllocator) {
        val field = Fields.I64.toArrowField("v")
        Vector.fromField(al, field).use { v ->
            assertEquals(field, v.field)
            v.maybePromote(al, Fields.I64.nullable.fieldType)
            assertEquals(Fields.I64.nullable.toArrowField("v"), v.field)
        }

        val field2 = Fields.I64.nullable.toArrowField("v")
        Vector.fromField(al, field2).use { v ->
            assertEquals(field2, v.field)
            v.maybePromote(al, Fields.I64.nullable.fieldType)
            assertEquals(field2, v.field)
        }
    }

    @Test
    fun `promotes to union`(al: BufferAllocator) {
        Vector.fromField(al, Fields.I32.toArrowField("v")).closeOnCatch { v ->
            v.writeAll(listOf(1, 2, 3))
            v.maybePromote(al, Fields.UTF8.fieldType)
        }.use { promoted ->
            promoted.writeAll(listOf("4", 5, "6"))
            assertEquals(
                Fields.Union("i32" to Fields.I32, "utf8" to Fields.UTF8).toArrowField("v"),
                promoted.field
            )
            assertEquals(listOf(1, 2, 3, "4", 5, "6"), promoted.toList())
        }

        Vector.fromField(al, Fields.List(Fields.I32).toArrowField("v"))
            .closeOnCatch { v ->
                v.writeAll(listOf(listOf(1, 2), listOf(3)))
                v.maybePromote(al, Fields.UTF8.fieldType)
            }
            .use { promoted ->
                promoted.writeAll(listOf("hello", listOf(4, 5), "world"))

                assertEquals(listOf(listOf(1, 2), listOf(3), "hello", listOf(4, 5), "world"), promoted.toList())
                assertEquals(Fields.Union("list" to Fields.List(Fields.I32), "utf8" to Fields.UTF8).toArrowField("v"), promoted.field)
            }
    }
}