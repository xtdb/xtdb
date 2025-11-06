package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.Vector.Companion.openVector
import xtdb.test.AllocatorResolver
import xtdb.types.Type
import xtdb.types.Type.Companion.asStructOf
import xtdb.types.Type.Companion.asUnionOf
import xtdb.types.Type.Companion.ofType

@ExtendWith(AllocatorResolver::class)
class ConcatVectorTest {

    @Test
    fun testMonomorphicSimpleVectors(alloc: BufferAllocator) {
        IntVector.open(alloc, "my-int", false).use { intVec1 ->
            IntVector.open(alloc, "my-int", false).use { intVec2 ->
                intVec1.writeInt(0)
                intVec1.writeInt(1)

                intVec2.writeInt(2)
                intVec2.writeInt(3)

                val concatRdr = ConcatVector.from("my-int", listOf(intVec1, intVec2))

                val r = 0..<4
                assertEquals(r.toList(), r.map { concatRdr.getInt(it) })

                val valueRdr = concatRdr.valueReader()
                assertEquals(
                    r.toList(),
                    r.map {
                        valueRdr.pos = it
                        valueRdr.readInt()
                    })

                IntVector.open(alloc, "my-int", false).use { resVec ->
                    val rowCopier = concatRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }
                    assertEquals(r.toList(), resVec.asList)
                }
            }
        }
    }

    private fun readMaps(valueReader: ValueReader): Any? =
        when (val o = valueReader.readObject()) {
            is Map<*, *> -> o.mapValues { readMaps(it.value as ValueReader) }
            else -> o
        }

    @Test
    fun testMonomorphicStructVectors(alloc: BufferAllocator) {
        val m1 = mapOf("foo" to false, "bar" to true)
        val m2 = mapOf("foo" to true, "bar" to false)

        Vector.fromList(alloc, "s1", listOf(m1, m1)).use { structVec1 ->
            Vector.fromList(alloc, "s2", listOf(m2, m2)).use { structVec2 ->

                val concatRdr = ConcatVector.from("my-struct", listOf(structVec1, structVec2))

                val r = 0..3
                val expected = listOf(m1, m1, m2, m2)
                assertEquals(expected, concatRdr.asList)

                val valueRdr = concatRdr.valueReader()
                assertEquals(expected, r.map {
                    valueRdr.pos = it
                    readMaps(valueRdr)
                })

                Vector.fromList(alloc, "res".asStructOf(), emptyList<Any?>()).use { resVec ->
                    val rowCopier = concatRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }
                    assertEquals(expected, resVec.asList)
                }
            }
        }
    }

    @Test
    fun testPolymorphicSimpleVectors(alloc: BufferAllocator) {
        Vector.fromList(alloc, "i32", listOf(0, 1)).use { intVec ->
            Vector.fromList(alloc, "utf8", listOf("first", "second")).use { stringVec ->

                val concatRdr = ConcatVector.from("my-duv", listOf(intVec, stringVec))

                val r = 0..3
                val expected = listOf(0, 1, "first", "second")
                assertEquals(expected, concatRdr.asList)

                val valueRdr = concatRdr.valueReader()
                assertEquals(expected, r.map {
                    valueRdr.pos = it
                    valueRdr.readObject()
                })

                Vector.fromList(alloc, "res".asUnionOf(), emptyList<Any?>()).use { resVec ->
                    val rowCopier = concatRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }
                    assertEquals(expected, resVec.asList)
                }
            }
        }
    }

    @Test
    fun testPolymorphicSimpleAndComplexVectors(alloc: BufferAllocator) {
        Vector.fromList(alloc, "i32", listOf(0, 3)).use { intVec ->
            Vector.fromList(alloc, "utf8", listOf("first", "fourth")).use { stringVec ->
                Vector.fromList(alloc, "duv", listOf(2, "fifth")).use { duvVec ->

                    val concatRdr = ConcatVector.from("my-duv", listOf(intVec, stringVec, duvVec))

                    val r = 0..5
                    val expected = listOf(0, 3, "first", "fourth", 2, "fifth")
                    assertEquals(expected, concatRdr.asList)

                    val valueRdr = concatRdr.valueReader()
                    assertEquals(expected, r.map {
                        valueRdr.pos = it
                        valueRdr.readObject()
                    })

                    Vector.fromList(alloc, "res".asUnionOf(), emptyList<Any?>()).use { resVec ->
                        val rowCopier = concatRdr.rowCopier(resVec)
                        r.forEach { rowCopier.copyRow(it) }

                        assertEquals(expected, resVec.asList)
                    }
                }
            }
        }
    }

    @Test
    fun testAbsentVectors(alloc: BufferAllocator) {
        Vector.fromList(alloc, "duv1", listOf(0, null, "fourth")).use { duv1 ->
            Vector.fromList(alloc, "duv2", listOf("first", 3, null)).use { duv2 ->

                val concatRdr = ConcatVector.from("my-duv", listOf(duv1, duv2))

                val r = 0..5
                val expected = listOf(0, null, "fourth", "first", 3, null)
                assertEquals(expected, concatRdr.asList)

                val valueRdr = concatRdr.valueReader()
                assertEquals(expected, r.map {
                    valueRdr.pos = it
                    val res = valueRdr.readObject()
                    res
                })

                Vector.fromList(alloc, "res".asUnionOf(), emptyList<Any?>()).use { resVec ->
                    val rowCopier = concatRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }

                    assertEquals(expected, resVec.asList)
                }
            }
        }
    }

    @Test
    fun testSingleLeggedDUVs(alloc: BufferAllocator) {
        Vector.fromList(alloc, "duv1".asUnionOf("i32" ofType Type.I32), listOf(0, 1)).use { duvVec1 ->
            val concatRdr = ConcatVector.from("my-duv", listOf(duvVec1))

            val r = 0..1
            val expected = listOf(0, 1)
            assertEquals(expected, concatRdr.asList)

            val valueRdr = concatRdr.valueReader()
            assertEquals(expected, r.map {
                valueRdr.pos = it
                val res = valueRdr.readInt()
                res
            })
        }
    }

    @Test
    fun testVectorForOrNull(alloc: BufferAllocator) {
        Vector.fromList(alloc, "s1", listOf(mapOf("foo" to true))).use { structVec1 ->
            Vector.fromList(alloc, "s2", listOf(mapOf("bar" to false))).use { structVec2 ->

                val concatRdr = ConcatVector.from("my-struct", listOf(structVec1, structVec2))

                // Access foo - should exist in vec1, null in vec2
                val fooRdr = concatRdr["foo"]
                assertEquals(true, fooRdr.getBoolean(0))
                assertEquals(true, fooRdr.isNull(1)) // NullVector for vec2

                // Access bar - should be null in vec1, exist in vec2
                val barRdr = concatRdr["bar"]
                assertEquals(true, barRdr.isNull(0)) // NullVector for vec1
                assertEquals(false, barRdr.getBoolean(1))
            }
        }
    }

    @Test
    fun testListVectors(alloc: BufferAllocator) {
        Vector.fromList(alloc, "l1", listOf(listOf(1, 2), listOf(3))).use { listVec1 ->
            Vector.fromList(alloc, "l2", listOf(listOf(4, 5, 6), listOf(7))).use { listVec2 ->
                val concatRdr = ConcatVector.from("my-list", listOf(listVec1, listVec2))

                // Test getListCount
                assertEquals(2, concatRdr.getListCount(0)) // [1, 2]
                assertEquals(1, concatRdr.getListCount(1)) // [3]
                assertEquals(3, concatRdr.getListCount(2)) // [4, 5, 6]
                assertEquals(1, concatRdr.getListCount(3)) // [7]

                // Test getListStartIndex - should account for cumulative offsets
                assertEquals(0, concatRdr.getListStartIndex(0)) // starts at 0
                assertEquals(2, concatRdr.getListStartIndex(1)) // starts at 2 (after [1,2])
                assertEquals(3, concatRdr.getListStartIndex(2)) // starts at 3 (after [1,2], [3])
                assertEquals(6, concatRdr.getListStartIndex(3)) // starts at 6 (after [1,2], [3], [4,5,6])

                // Test listElements reader
                val listElems = concatRdr.listElements
                assertEquals(7, listElems.valueCount) // total elements: 2 + 1 + 3 + 1
                assertEquals((1..7).toList(), (0..6).map { listElems.getInt(it) })

                // Test reading full objects
                assertEquals(listOf(1, 2), concatRdr[0])
                assertEquals(listOf(3), concatRdr[1])
                assertEquals(listOf(4, 5, 6), concatRdr[2])
                assertEquals(listOf(7), concatRdr[3])
                assertEquals(listOf(listOf(1, 2), listOf(3), listOf(4, 5, 6), listOf(7)), concatRdr.asList)
            }
        }
    }

    @Test
    fun testRowCopierCopyRange(alloc: BufferAllocator) {
        // vec1: [0, 1], vec2: [2, 3, 4], vec3: [5, 6]
        Vector.fromList(alloc, "v1", listOf(0, 1)).use { vec1 ->
            Vector.fromList(alloc, "v2", listOf(2, 3, 4)).use { vec2 ->
                Vector.fromList(alloc, "v3", listOf(5, 6)).use { vec3 ->
                    val concatRdr = ConcatVector.from("my-int", listOf(vec1, vec2, vec3))

                    // Test range entirely within single reader
                    ("res" ofType Type.I32).openVector(alloc).use { resVec ->
                        val rowCopier = concatRdr.rowCopier(resVec)
                        rowCopier.copyRange(2, 3)
                        assertEquals(listOf(2, 3, 4), resVec.asList)
                    }

                    // Test range spanning multiple readers
                    ("res" ofType Type.I32).openVector(alloc).use { resVec ->
                        val rowCopier = concatRdr.rowCopier(resVec)
                        rowCopier.copyRange(1, 5)
                        assertEquals(listOf(1, 2, 3, 4, 5), resVec.asList)
                    }

                    // Test range spanning all readers
                    ("res" ofType Type.I32).openVector(alloc).use { resVec ->
                        val rowCopier = concatRdr.rowCopier(resVec)
                        rowCopier.copyRange(0, 7)
                        assertEquals(listOf(0, 1, 2, 3, 4, 5, 6), resVec.asList)
                    }
                }
            }
        }
    }

    @Test
    fun testEmptyVectorsFiltered(alloc: BufferAllocator) {
        Vector.fromList(alloc, "v1", listOf(1, 2)).use { vec1 ->
            Vector.fromList(alloc, "v2" ofType Type.I32, emptyList<Int>()).use { vec2 ->
                Vector.fromList(alloc, "v3", listOf(3, 4)).use { vec3 ->
                    Vector.fromList(alloc, "v4", emptyList<Int>()).use { vec4 ->

                        val concatRdr = ConcatVector.from("my-int", listOf(vec1, vec2, vec3, vec4))

                        // Empty vectors should be filtered out, leaving only vec1 and vec3
                        assertEquals(4, concatRdr.valueCount)
                        assertEquals(listOf(1, 2, 3, 4), concatRdr.asList)

                        // Test that indexing works correctly across the gap
                        assertEquals(1, concatRdr.getInt(0))
                        assertEquals(2, concatRdr.getInt(1))
                        assertEquals(3, concatRdr.getInt(2))
                        assertEquals(4, concatRdr.getInt(3))
                    }
                }
            }
        }

        // Test with only empty vectors
        Vector.fromList(alloc, "empty1" ofType Type.I32, emptyList<Int>()).use { empty1 ->
            Vector.fromList(alloc, "empty2" ofType Type.I32, emptyList<Int>()).use { empty2 ->
                val emptyConcat = ConcatVector.from("all-empty", listOf(empty1, empty2))
                assertEquals(0, emptyConcat.valueCount)
                assertEquals(emptyList<Int>(), emptyConcat.asList)
            }
        }
    }
}
