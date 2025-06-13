package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DenseUnionVectorTest {
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun testDenseUnionVector() {
        DenseUnionVector(
            allocator, "duv",
            listOf(
                IntVector(allocator, "i32", false),
                Utf8Vector(allocator, "utf8", true)
            )
        ).use { duv ->
            val i32Leg = duv.vectorFor("i32")
            val utf8Leg = duv.vectorFor("utf8")

            i32Leg.writeInt(12)
            utf8Leg.writeObject("hello")
            utf8Leg.writeObject("world!")
            i32Leg.writeInt(34)
            utf8Leg.writeNull()

            assertEquals(5, duv.valueCount)
            assertEquals(listOf(12, 34), i32Leg.toList())
            assertEquals(listOf("hello", "world!", null), utf8Leg.toList())
            assertEquals(listOf(12, "hello", "world!", 34, null), duv.toList())

            assertEquals(listOf(0, 1, 1, 0, 1).map { it.toByte() }, duv.typeIds())
            assertEquals(listOf(0, 0, 1, 1, 2), duv.offsets())
        }
    }

    @Test
    fun `scalar into DUV #3609`() {
        IntVector(allocator, "my-int", false).use { myIntVec ->
            myIntVec.writeInt(32)
            myIntVec.writeInt(64)

            DenseUnionVector(
                allocator, "dest",
                listOf(
                    Utf8Vector(allocator, "utf8", false),
                    IntVector(allocator, "i32", true)
                )
            ).use { destVec ->
                myIntVec.rowCopier(destVec).run {
                    copyRow(1)
                    copyRow(0)
                }

                assertEquals(listOf(64, 32), destVec.toList())
            }
        }
    }

    @Test
    fun `nullable mono into DUV`() {
        DoubleVector(allocator, "dbl", true).use { dblVec ->
            dblVec.writeDouble(3.14)
            dblVec.writeNull()
            dblVec.writeDouble(2.71)

            DenseUnionVector(
                allocator, "dest",
                listOf(DoubleVector(allocator, "f64", true))
            ).use { destVec ->
                dblVec.rowCopier(destVec).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(3.14, null, 2.71), destVec.toList())
            }
        }
    }


    @Test
    fun `from null into duv vector`() {
        NullVector("v1").use { nullVector ->
            nullVector.writeNull()
            nullVector.writeNull()
            nullVector.writeNull()

            DenseUnionVector(
                allocator, "v2",
                listOf(LongVector(allocator, "i64", true))
            ).use { copy ->
                val copier = nullVector.rowCopier(copy)
                copier.copyRow(0)
                copier.copyRow(1)
                copier.copyRow(2)

                assertEquals(3, copy.valueCount)
                assertNull(copy.getObject(1))
            }
        }
    }

    @Test
    fun `from nullable duv vector to mono vector`() {
        DenseUnionVector(
            allocator, "v1",
            listOf(
                NullVector("null"),
                IntVector(allocator, "i32", false),
            )
        ).use { duv ->
            val intLeg = duv.vectorFor("i32")
            val nullLeg = duv.vectorFor("null")
            intLeg.writeInt(12)
            nullLeg.writeNull()
            intLeg.writeInt(34)

            assertEquals(3, duv.valueCount)

            IntVector(allocator, "mono", true).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(12, null, 34), mono.toList())
            }
        }

        DenseUnionVector(
            allocator, "v1",
            listOf(
                NullVector("null"),
                StructVector(allocator, "struct", false,
                    linkedMapOf(
                        "a" to IntVector(allocator, "i32", false),
                        "b" to IntVector(allocator, "i32", false)
                    )
                ),
            )
        ).use { duv ->
            val obj1 = mapOf("a" to 1, "b" to 2)
            val obj2 = mapOf("a" to 3, "b" to 4)

            val structLeg = duv.vectorFor("struct")
            val nullLeg = duv.vectorFor("null")
            structLeg.writeObject(obj1)
            nullLeg.writeNull()
            structLeg.writeObject(obj2)

            assertEquals(3, duv.valueCount)

            StructVector(allocator, "mono", true,
                linkedMapOf(
                    "a" to IntVector(allocator, "i32", false),
                    "b" to IntVector(allocator, "i32", false)
                )
            ).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(obj1, null, obj2), mono.toList())
            }
        }

        DenseUnionVector(
            allocator, "v1",
            listOf(
                NullVector("null"),
                ListVector(allocator, "list", false, IntVector(allocator, "i32", false))
            ),
        )
        .use { duv ->
            val obj1 = listOf(1, 2)
            val obj2 = emptyList<Int>()

            val structLeg = duv.vectorFor("list")
            val nullLeg = duv.vectorFor("null")
            structLeg.writeObject(obj1)
            nullLeg.writeNull()
            structLeg.writeObject(obj2)

            assertEquals(3, duv.valueCount)

            ListVector(allocator, "list", false, IntVector(allocator, "i32", false)).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(obj1, null, obj2), mono.toList())
            }
        }
    }
}