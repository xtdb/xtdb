package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.Types.MinorType.*
import org.apache.arrow.vector.types.pojo.ArrowType.Bool
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

fun <T> cycle(list: List<T>) = sequence { while (true) yieldAll(list) }

@ExtendWith(AllocatorResolver::class)
class MultiVectorReaderTest {

    @Test
    fun testMonomorphicSimpleVectors(alloc: BufferAllocator) {
        IntVector(alloc, "my-int", false).use { intVec1 ->
            IntVector(alloc, "my-int", false).use { intVec2 ->
                for (i in 0..4) {
                    if (i % 2 == 0) intVec1.writeInt(i)
                    else intVec2.writeInt(i)
                }

                val indirectRdr = MultiVectorReader(
                    listOf(intVec1, intVec2),
                    VectorIndirection.selection(intArrayOf(0, 1, 0, 1)),
                    VectorIndirection.selection(intArrayOf(0, 0, 1, 1))
                )
                val r = 0..<4
                assertEquals(r.toList(), r.map { indirectRdr.getInt(it) })

                val pos = VectorPosition.build(0)
                val valueRdr = indirectRdr.valueReader(pos)
                assertEquals(r.toList(), r.map { valueRdr.readInt().also { pos.getPositionAndIncrement() } })

                IntVector(alloc, "my-int", false).use { resVec ->
                    val rowCopier = indirectRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }
                    assertEquals(r.toList(), resVec.toList())
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
        val fooField = Field("foo", FieldType.notNullable(Bool.INSTANCE), null)
        val barField = Field("bar", FieldType.notNullable(Bool.INSTANCE), null)
        val structField =
            Field("my-struct", FieldType(false, Struct.INSTANCE, null, null), listOf(fooField, barField))

        Vector.fromField(alloc, structField).use { structVec1 ->
            Vector.fromField(alloc, structField).use { structVec2 ->
                val m1 = mapOf("foo" to false, "bar" to true)
                val m2 = mapOf("foo" to true, "bar" to false)

                for (i in 0..4) {
                    if (i % 2 == 0) structVec1.writeObject(m1)
                    else structVec2.writeObject(m2)
                }
                val indirectRdr = MultiVectorReader(
                    listOf(structVec1, structVec2),
                    VectorIndirection.selection(intArrayOf(0, 1, 0, 1)),
                    VectorIndirection.selection(intArrayOf(0, 0, 1, 1))
                )
                val r = 0..3
                val expected = cycle(listOf(m1, m2)).take(4).toList()
                assertEquals(expected, r.map { indirectRdr.getObject(it) })

                val pos = VectorPosition.build(0)
                val valueRdr = indirectRdr.valueReader(pos)
                assertEquals(expected, r.map { readMaps(valueRdr).also { pos.getPositionAndIncrement() } })

                Vector.fromField(alloc, structField).use { resVec ->
                    val rowCopier = indirectRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }
                    assertEquals(expected, resVec.toList())
                }
            }
        }
    }

    @Test
    fun testPolymorphicSimpleVectors(alloc: BufferAllocator) {
        IntVector(alloc, "i32", false).use { intVec ->
            intVec.writeInt(0)
            intVec.writeInt(1)

            Utf8Vector(alloc, "utf8", false).use { stringVec ->
                stringVec.writeObject("first")
                stringVec.writeObject("second")

                val indirectRdr = MultiVectorReader(
                    listOf(intVec, stringVec),
                    VectorIndirection.selection(intArrayOf(0, 1, 0, 1)),
                    VectorIndirection.selection(intArrayOf(0, 0, 1, 1))
                )

                val r = 0..3
                val expected = listOf(0, "first", 1, "second")
                assertEquals(expected, indirectRdr.toList())

                val pos = VectorPosition.build(0)
                val valueRdr = indirectRdr.valueReader(pos)
                assertEquals(expected, r.map { valueRdr.readObject().also { pos.getPositionAndIncrement() } })

                val duvField = Field(
                    "my-duv", FieldType.notNullable(DENSEUNION.type),
                    listOf(intVec.field, stringVec.field)
                )

                Vector.fromField(alloc, duvField).use { resVec ->
                    val rowCopier = indirectRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }
                    assertEquals(expected, resVec.toList())
                }
            }
        }
    }

    @Test
    fun testPolymorphicSimpleAndComplexVectors(alloc: BufferAllocator) {
        IntVector(alloc, "i32", false).use { intVec ->
            intVec.writeInt(0)
            intVec.writeInt(3)

            Utf8Vector(alloc, "utf8", false).use { stringVec ->
                stringVec.writeObject("first")
                stringVec.writeObject("fourth")

                val duvField = Field(
                    "my-duv", FieldType.notNullable(DENSEUNION.type),
                    listOf(intVec.field, stringVec.field)
                )

                Vector.fromField(alloc, duvField).use { duvVec ->
                    duvVec.vectorFor("i32").writeInt(2)
                    duvVec.vectorFor("utf8").writeObject("fifth")

                    val indirectRdr = MultiVectorReader(
                        listOf(intVec, stringVec, duvVec),
                        VectorIndirection.selection(intArrayOf(0, 1, 2, 0, 1, 2)),
                        VectorIndirection.selection(intArrayOf(0, 0, 0, 1, 1, 1))
                    )

                    val r = 0..5
                    val expected = listOf(0, "first", 2, 3, "fourth", "fifth")
                    assertEquals(expected, indirectRdr.toList())

                    val pos = VectorPosition.build(0)
                    val valueRdr = indirectRdr.valueReader(pos)
                    assertEquals(expected, r.map { valueRdr.readObject().also { pos.getPositionAndIncrement() } })

                    Vector.fromField(alloc, duvField).use { resVec ->
                        val rowCopier = indirectRdr.rowCopier(resVec)
                        r.forEach { rowCopier.copyRow(it) }

                        assertEquals(expected, resVec.toList())
                    }
                }
            }
        }
    }

    @Test
    fun testAbsentVectors(alloc: BufferAllocator) {
        val duvField = Field(
            "my-duv", FieldType(false, DENSEUNION.type, null, null),
            listOf(
                Field("i32", FieldType.notNullable(INT.type), null),
                Field("utf8", FieldType.notNullable(VARCHAR.type), null),
                Field("null", FieldType.nullable(NULL.type), null),
            )
        )

        Vector.fromField(alloc, duvField).use { duv1 ->
            Vector.fromField(alloc, duvField).use { duv2 ->
                duv1.vectorFor("i32").writeInt(0)
                duv2.vectorFor("utf8").writeObject("first")
                duv1.vectorFor("null").writeNull()
                duv2.vectorFor("i32").writeInt(3)
                duv1.vectorFor("utf8").writeObject("fourth")
                duv2.vectorFor("null").writeNull()

                val indirectRdr = MultiVectorReader(
                    listOf(duv1, duv2),
                    VectorIndirection.selection(intArrayOf(0, 1, 0, 1, 0, 1)),
                    VectorIndirection.selection(intArrayOf(0, 0, 1, 1, 2, 2))
                )
                val r = 0..5
                val expected = listOf(0, "first", null, 3, "fourth", null)
                assertEquals(expected, indirectRdr.toList())

                val pos = VectorPosition.build(0)
                val valueRdr = indirectRdr.valueReader(pos)
                assertEquals(expected, r.map {
                    val res = valueRdr.readObject()
                    pos.getPositionAndIncrement()
                    res
                })

                Vector.fromField(alloc, duvField).use { resVec ->
                    val rowCopier = indirectRdr.rowCopier(resVec)
                    r.forEach { rowCopier.copyRow(it) }

                    assertEquals(expected, resVec.toList())
                }
            }
        }
    }

    @Test
    fun testSingleLeggedDUVs(alloc: BufferAllocator) {
        val duvField = Field(
            "my-duv", FieldType(false, DENSEUNION.type, null, null),
            listOf(Field("i32", FieldType.notNullable(INT.type), emptyList()))
        )

        Vector.fromField(alloc, duvField).use { duvVec1 ->
            duvVec1.vectorFor("i32").run { writeInt(0); writeInt(1) }

            val indirectRdr = MultiVectorReader(
                listOf(duvVec1),
                VectorIndirection.selection(intArrayOf(0, 0)),
                VectorIndirection.selection(intArrayOf(0, 1))
            )

            val r = 0..1
            val expected = listOf(0, 1)
            assertEquals(expected, indirectRdr.toList())

            val pos = VectorPosition.build(0)
            val valueRdr = indirectRdr.valueReader(pos)
            assertEquals(expected, r.map {
                val res = valueRdr.readInt()
                pos.getPositionAndIncrement()
                res
            })
        }
    }
}