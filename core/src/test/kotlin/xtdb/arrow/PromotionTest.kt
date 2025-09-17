package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import xtdb.types.Type
import xtdb.types.Type.Companion.I32
import xtdb.types.Type.Companion.I64
import xtdb.types.Type.Companion.UTF8
import xtdb.types.Type.Companion.asListOf
import xtdb.types.Type.Companion.asStructOf
import xtdb.types.Type.Companion.asUnionOf
import xtdb.types.Type.Companion.just
import xtdb.types.Type.Companion.maybe
import xtdb.types.Type.Companion.ofType
import xtdb.util.closeOnCatch

@ExtendWith(AllocatorResolver::class)
class PromotionTest {
    @Test
    fun `no-op if the type matches`(al: BufferAllocator) {
        Vector.fromField(al, "i64" ofType I64)
            .use { v -> assertEquals(v, v.maybePromote(al, I64.fieldType)) }

        Vector.fromField(al, "struct".asStructOf("a" ofType I64, "b" ofType UTF8))
            .use { v -> assertEquals(v, v.maybePromote(al, just(STRUCT).fieldType)) }
    }

    @Test
    fun `adds nullable, but otherwise same vec`(al: BufferAllocator) {
        val field = "v" ofType I64
        Vector.fromField(al, field).use { v ->
            assertEquals(field, v.field)
            v.maybePromote(al, maybe(I64).fieldType)
            assertEquals("v" ofType maybe(I64), v.field)
        }

        val field2 = "v" ofType maybe(I64)
        Vector.fromField(al, field2).use { v ->
            assertEquals(field2, v.field)
            v.maybePromote(al, maybe(I64).fieldType)
            assertEquals(field2, v.field)
        }
    }

    @Test
    fun `promotes to union`(al: BufferAllocator) {
        Vector.fromField(al, "v" ofType I32).closeOnCatch { v ->
            v.writeAll(listOf(1, 2, 3))
            v.maybePromote(al, UTF8.fieldType)
        }.use { promoted ->
            promoted.writeAll(listOf("4", 5, "6"))
            assertEquals(
                "v".asUnionOf("i32" ofType I32, "utf8" ofType UTF8),
                promoted.field
            )
            assertEquals(listOf(1, 2, 3, "4", 5, "6"), promoted.toList())
        }

        Vector.fromField(al, "v".asListOf(I32))
            .closeOnCatch { v ->
                v.writeAll(listOf(listOf(1, 2), listOf(3)))
                v.maybePromote(al, UTF8.fieldType)
            }
            .use { promoted ->
                promoted.writeAll(listOf("hello", listOf(4, 5), "world"))

                assertEquals(listOf(listOf(1, 2), listOf(3), "hello", listOf(4, 5), "world"), promoted.toList())
                assertEquals(
                    "v".asUnionOf("list" asListOf I32, "utf8" ofType UTF8),
                    promoted.field
                )
            }
    }

    @Test
    fun `rowCopier throws on invalid-copy-source`(al: BufferAllocator) {
        Vector.fromField(al, "src" ofType I32).use { src ->
            Vector.fromField(al, "dest" ofType UTF8).closeOnCatch { dest ->
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
        Vector.fromField(al, "src".asStructOf("a" ofType I32)).use { src ->
            Vector.fromField(al, "dest".asStructOf("a" ofType UTF8)).use { dest ->
                src.writeObject(mapOf("a" to 1))
                dest.writeObject(mapOf("a" to "hello"))
                val copier = src.rowCopier(dest)
                copier.copyRow(0)

                assertEquals(listOf(mapOf("a" to "hello"), mapOf("a" to 1)), dest.toList())
            }
        }

        Vector.fromField(al, "src".asStructOf("a" ofType I32)).use { src ->
            Vector.fromField(al, "dest".asStructOf("b" ofType I32)).use { dest ->
                src.writeObject(mapOf("a" to 4))
                dest.writeObject(mapOf("b" to 10))
                val copier = src.rowCopier(dest)

                assertEquals(
                    "dest".asStructOf("b" ofType maybe(I32), "a" ofType maybe(I32)),
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
            Vector.open(al, "dest", Type.listTypeOf(Type.NULL)).use { destVec ->
                srcVec.rowCopier(destVec).copyRow(0)

                assertEquals(listOf(listOf(1)), destVec.toList())
                assertEquals("dest" ofType Type.listTypeOf(I32), destVec.field)
            }
        }
    }
}