package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import xtdb.types.Type
import xtdb.util.closeOnCatch

@ExtendWith(AllocatorResolver::class)
class PromotionTest {
    @Test
    fun `no-op if the type matches`(al: BufferAllocator) {
        Vector.fromField(al, Type.I64.toField("i64"))
            .use { v -> assertEquals(v, v.maybePromote(al, Type.I64.fieldType)) }

        Vector.fromField(al, Type.struct("a" to Type.I64, "b" to Type.UTF8).toField("struct"))
            .use { v -> assertEquals(v, v.maybePromote(al, Type.struct().fieldType)) }
    }

    @Test
    fun `adds nullable, but otherwise same vec`(al: BufferAllocator) {
        val field = Type.I64.toField("v")
        Vector.fromField(al, field).use { v ->
            assertEquals(field, v.field)
            v.maybePromote(al, Type.I64.nullable().fieldType)
            assertEquals(Type.I64.nullable().toField("v"), v.field)
        }

        val field2 = Type.I64.nullable().toField("v")
        Vector.fromField(al, field2).use { v ->
            assertEquals(field2, v.field)
            v.maybePromote(al, Type.I64.nullable().fieldType)
            assertEquals(field2, v.field)
        }
    }

    @Test
    fun `promotes to union`(al: BufferAllocator) {
        Vector.fromField(al, Type.I32.toField("v")).closeOnCatch { v ->
            v.writeAll(listOf(1, 2, 3))
            v.maybePromote(al, Type.UTF8.fieldType)
        }.use { promoted ->
            promoted.writeAll(listOf("4", 5, "6"))
            assertEquals(
                Type.union("i32" to Type.I32, "utf8" to Type.UTF8).toField("v"),
                promoted.field
            )
            assertEquals(listOf(1, 2, 3, "4", 5, "6"), promoted.toList())
        }

        Vector.fromField(al, Type.list(Type.I32).toField("v"))
            .closeOnCatch { v ->
                v.writeAll(listOf(listOf(1, 2), listOf(3)))
                v.maybePromote(al, Type.UTF8.fieldType)
            }
            .use { promoted ->
                promoted.writeAll(listOf("hello", listOf(4, 5), "world"))

                assertEquals(listOf(listOf(1, 2), listOf(3), "hello", listOf(4, 5), "world"), promoted.toList())
                assertEquals(
                    Type.union("list" to Type.list(Type.I32), "utf8" to Type.UTF8).toField("v"),
                    promoted.field
                )
            }
    }

    @Test
    fun `rowCopier throws on invalid-copy-source`(al: BufferAllocator) {
        Vector.fromField(al, Type.I32.toField("src")).use { src ->
            Vector.fromField(al, Type.UTF8.toField("dest")).closeOnCatch { dest ->
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
        Vector.fromField(al, Type.struct("a" to Type.I32).toField("src")).use { src ->
            Vector.fromField(al, Type.struct("a" to Type.UTF8).toField("dest")).use { dest ->
                src.writeObject(mapOf("a" to 1))
                dest.writeObject(mapOf("a" to "hello"))
                val copier = src.rowCopier(dest)
                copier.copyRow(0)

                assertEquals(listOf(mapOf("a" to "hello"), mapOf("a" to 1)), dest.toList())
            }
        }

        Vector.fromField(al, Type.struct("a" to Type.I32).toField("src")).use { src ->
            Vector.fromField(al, Type.struct("b" to Type.I32).toField("dest")).use { dest ->
                src.writeObject(mapOf("a" to 4))
                dest.writeObject(mapOf("b" to 10))
                val copier = src.rowCopier(dest)

                assertEquals(
                    Type.struct("b" to Type.I32.nullable(), "a" to Type.I32.nullable()).toField("dest"),
                    dest.field
                )

                copier.copyRow(0)

                assertEquals(listOf(mapOf("b" to 10), mapOf("a" to 4)), dest.toList())
            }
        }
    }

    @Test
    fun `rowCopier in a list-vec promotes the el-vector`(al: BufferAllocator) {
        Vector.fromList(al, "src", listOf(listOf(1))).use { srcVec ->
            Vector.open(al, "dest", Type.list(Type.NULL)).use { destVec ->
                srcVec.rowCopier(destVec).copyRow(0)

                assertEquals(listOf(listOf(1)), destVec.toList())
                assertEquals(Type.list(Type.I32).toField("dest"), destVec.field)
            }
        }
    }
}