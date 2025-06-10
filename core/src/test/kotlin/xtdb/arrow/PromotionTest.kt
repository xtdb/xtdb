package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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
                assertEquals(
                    Fields.Union("list" to Fields.List(Fields.I32), "utf8" to Fields.UTF8).toArrowField("v"),
                    promoted.field
                )
            }
    }

    @Test
    fun `rowCopier throws on invalid-copy-source`(al: BufferAllocator) {
        Vector.fromField(al, Fields.I32.toArrowField("src")).use { src ->
            Vector.fromField(al, Fields.UTF8.toArrowField("dest")).closeOnCatch { dest ->
                dest.writeAll(listOf("hello", "world"))
                src.writeAll(listOf(1, 2))
                assertThrows<InvalidCopySourceException> { src.rowCopier(dest) }
                dest.maybePromote(al, src.fieldType)
            }.use { newDest ->
                val copier = src.rowCopier(newDest)
                copier.copyRow(0)
                copier.copyRow(1)

                assertEquals(listOf("hello", "world", 1, 2), newDest.toList())
            }
        }
    }

    @Test
    fun `rowCopier within a struct promotes the child-vecs`(al: BufferAllocator) {
        Vector.fromField(al, Fields.Struct("a" to Fields.I32).toArrowField("src")).use { src ->
            Vector.fromField(al, Fields.Struct("a" to Fields.UTF8).toArrowField("dest")).use { dest ->
                src.writeObject(mapOf("a" to 1))
                dest.writeObject(mapOf("a" to "hello"))
                val copier = src.rowCopier(dest)
                copier.copyRow(0)

                assertEquals(listOf(mapOf("a" to "hello"), mapOf("a" to 1)), dest.toList())
            }
        }

        Vector.fromField(al, Fields.Struct("a" to Fields.I32).toArrowField("src")).use { src ->
            Vector.fromField(al, Fields.Struct("b" to Fields.I32).toArrowField("dest")).use { dest ->
                src.writeObject(mapOf("a" to 4))
                dest.writeObject(mapOf("b" to 10))
                val copier = src.rowCopier(dest)

                assertEquals(
                    Fields.Struct("b" to Fields.I32.nullable, "a" to Fields.I32.nullable).toArrowField("dest"),
                    dest.field
                )

                copier.copyRow(0)

                assertEquals(listOf(mapOf("b" to 10), mapOf("a" to 4)), dest.toList())
            }
        }
    }
}