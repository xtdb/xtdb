package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.VectorType.Companion.structOf
import xtdb.arrow.VectorType.Companion.fromLegs
import xtdb.arrow.VectorType.Null
import xtdb.test.AllocatorResolver
import xtdb.util.closeOnCatch

@ExtendWith(AllocatorResolver::class)
class PromotionTest {
    @Test
    fun `no-op if the type matches`(al: BufferAllocator) {
        al.openVector("i64", I64)
            .use { v -> assertEquals(v, v.maybePromote(al, I64_TYPE, false)) }

        al.openVector("struct", structOf("a" to I64, "b" to UTF8))
            .use { v -> assertEquals(v, v.maybePromote(al, STRUCT, false)) }
    }

    @Test
    fun `adds nullable, but otherwise same vec`(al: BufferAllocator) {
        val field = "v" ofType I64
        al.openVector(field).use { v ->
            assertEquals(field, v.field)
            v.maybePromote(al, I64_TYPE, true)
            assertEquals("v" ofType maybe(I64), v.field)
        }

        val field2 = "v" ofType maybe(I64)
        al.openVector(field2).use { v ->
            assertEquals(field2, v.field)
            v.maybePromote(al, I64_TYPE, true)
            assertEquals(field2, v.field)
        }
    }

    @Test
    fun `promotes to union`(al: BufferAllocator) {
        al.openVector("v", I32).closeOnCatch { v ->
            v.writeAll(listOf(1, 2, 3))
            v.maybePromote(al, UTF8_TYPE, false)
        }.use { promoted ->
            promoted.writeAll(listOf("4", 5, "6"))
            assertEquals(
                "v" ofType fromLegs(I32, UTF8),
                promoted.field
            )
            assertEquals(listOf(1, 2, 3, "4", 5, "6"), promoted.asList)
        }

        al.openVector("v", listTypeOf(I32))
            .closeOnCatch { v ->
                v.writeAll(listOf(listOf(1, 2), listOf(3)))
                v.maybePromote(al, UTF8_TYPE, false)
            }
            .use { promoted ->
                promoted.writeAll(listOf("hello", listOf(4, 5), "world"))

                assertEquals(listOf(listOf(1, 2), listOf(3), "hello", listOf(4, 5), "world"), promoted.asList)
                assertEquals(
                    "v" ofType fromLegs(listTypeOf(I32), UTF8),
                    promoted.field
                )
            }
    }

    @Test
    fun `rowCopier throws on invalid-copy-source`(al: BufferAllocator) {
        al.openVector("src", I32).use { src ->
            al.openVector("dest", UTF8).closeOnCatch { dest ->
                dest.writeAll(listOf("hello", "world"))
                src.writeAll(listOf(1, 2))
                assertThrows<InvalidCopySourceException> { src.rowCopier(dest) }
                dest.maybePromote(al, src.arrowType, src.nullable)
            }.use { newDest ->
                val copier = src.rowCopier(newDest)
                copier.copyRow(0)
                copier.copyRow(1)

                assertEquals(listOf("hello", "world", 1, 2), newDest.asList)
            }
        }
    }

    @Test
    fun `rowCopier within a struct promotes the child-vecs`(al: BufferAllocator) {
        al.openVector("src", structOf("a" to I32)).use { src ->
            al.openVector("dest", structOf("a" to UTF8)).use { dest ->
                src.writeObject(mapOf("a" to 1))
                dest.writeObject(mapOf("a" to "hello"))
                val copier = src.rowCopier(dest)
                copier.copyRow(0)

                assertEquals(listOf(mapOf("a" to "hello"), mapOf("a" to 1)), dest.asList)
            }
        }

        al.openVector("src", structOf("a" to I32)).use { src ->
            al.openVector("dest", structOf("b" to I32)).use { dest ->
                src.writeObject(mapOf("a" to 4))
                dest.writeObject(mapOf("b" to 10))
                val copier = src.rowCopier(dest)

                assertEquals(
                    "dest" ofType structOf("b" to maybe(I32), "a" to maybe(I32)),
                    dest.field
                )

                copier.copyRow(0)

                assertEquals(listOf(mapOf("b" to 10), mapOf("a" to 4)), dest.asList)
            }
        }
    }

    @Test
    fun `rowCopier in a list-vec promotes the el-vector`(al: BufferAllocator) {
        Vector.fromList(al, "src", listOf(listOf(1))).use { srcVec ->
            al.openVector("dest", listTypeOf(Null)).use { destVec ->
                srcVec.rowCopier(destVec).copyRow(0)

                assertEquals(listOf(listOf(1)), destVec.asList)
                assertEquals("dest" ofType listTypeOf(I32), destVec.field)
            }
        }
    }
}