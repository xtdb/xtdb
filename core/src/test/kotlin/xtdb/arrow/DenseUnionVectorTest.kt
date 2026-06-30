package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.TaggedValue
import xtdb.kw

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
                IntVector.open(allocator, "i32", false),
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
            assertEquals(listOf(12, 34), i32Leg.asList)
            assertEquals(listOf("hello", "world!", null), utf8Leg.asList)
            assertEquals(listOf(12, "hello", "world!", 34, null), duv.asList)

            assertEquals(listOf(0, 1, 1, 0, 1).map { it.toByte() }, duv.typeIds())
            assertEquals(listOf(0, 0, 1, 1, 2), duv.offsets())
        }
    }

    @Test
    fun `scalar into DUV #3609`() {
        IntVector.open(allocator, "my-int", false).use { myIntVec ->
            myIntVec.writeInt(32)
            myIntVec.writeInt(64)

            DenseUnionVector(
                allocator, "dest",
                listOf(
                    Utf8Vector(allocator, "utf8", false),
                    IntVector.open(allocator, "i32", true)
                )
            ).use { destVec ->
                myIntVec.rowCopier(destVec).run {
                    copyRow(1)
                    copyRow(0)
                }

                assertEquals(listOf(64, 32), destVec.asList)
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

                assertEquals(listOf(3.14, null, 2.71), destVec.asList)
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
                IntVector.open(allocator, "i32", false),
            )
        ).use { duv ->
            val intLeg = duv.vectorFor("i32")
            val nullLeg = duv.vectorFor("null")
            intLeg.writeInt(12)
            nullLeg.writeNull()
            intLeg.writeInt(34)

            assertEquals(3, duv.valueCount)

            IntVector.open(allocator, "mono", true).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(12, null, 34), mono.asList)
            }
        }

        DenseUnionVector(
            allocator, "v1",
            listOf(
                NullVector("null"),
                StructVector(allocator, "struct", false,
                    linkedMapOf(
                        "a" to IntVector.open(allocator, "i32", false),
                        "b" to IntVector.open(allocator, "i32", false)
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
                    "a" to IntVector.open(allocator, "i32", false),
                    "b" to IntVector.open(allocator, "i32", false)
                )
            ).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(obj1, null, obj2), mono.asList)
            }
        }

        DenseUnionVector(
            allocator, "v1",
            listOf(
                NullVector("null"),
                ListVector(allocator, "list", false, IntVector.open(allocator, "i32", false))
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

            ListVector(allocator, "list", false, IntVector.open(allocator, "i32", false)).use { mono ->
                duv.rowCopier(mono).run {
                    copyRow(0)
                    copyRow(1)
                    copyRow(2)
                }

                assertEquals(listOf(1, 0, 1).map { it.toByte() }, duv.typeIds())
                assertEquals(listOf(obj1, null, obj2), mono.asList)
            }
        }
    }

    // a put-less segment (e.g. erase-only) writes its `op` union with a Null `put` leg
    // alongside delete/erase; three legs, so a copy goes through rowCopier0/legVectorFor
    // rather than the single-leg `rowCopier` shortcut that bypasses it.
    private fun putLessOpDuv() =
        DenseUnionVector(allocator, "op",
            listOf(NullVector("put"), NullVector("delete"), NullVector("erase")))

    private fun structPutOpDuv() =
        DenseUnionVector(allocator, "op",
            listOf(StructVector(allocator, "put", true,
                linkedMapOf("a" to IntVector.open(allocator, "i32", true))),
                NullVector("delete"), NullVector("erase")))

    @Test
    fun `merging a put-less op-union into a Struct-put one reconciles the Null put leg #5714`() {
        structPutOpDuv().use { dest ->
            structPutOpDuv().use { withPut ->
                withPut.vectorFor("put").writeObject(mapOf("a" to 1))

                putLessOpDuv().use { putLess ->
                    putLess.vectorFor("erase").writeNull()

                    withPut.rowCopier(dest).copyRange(0, withPut.valueCount)
                    putLess.rowCopier(dest).copyRange(0, putLess.valueCount)

                    assertEquals(2, dest.valueCount)
                    assertEquals(TaggedValue("put".kw, mapOf("a" to 1)), dest.getObject(0))
                }
            }
        }
    }

    @Test
    fun `merging a Struct-put op-union into a put-less one promotes the Null put leg #5714`() {
        putLessOpDuv().use { dest ->
            structPutOpDuv().use { withPut ->
                withPut.vectorFor("put").writeObject(mapOf("a" to 7))
                withPut.rowCopier(dest).copyRange(0, withPut.valueCount)

                assertEquals(1, dest.valueCount)
                assertEquals(TaggedValue("put".kw, mapOf("a" to 7)), dest.getObject(0))
            }
        }
    }

    @Test
    fun `merging a put-less op-union keeps a non-nullable put leg non-nullable #5714`() {
        // non-nullable Struct put, mirroring a compaction output
        val putLeg = StructVector(allocator, "put", false,
            linkedMapOf("a" to IntVector.open(allocator, "i32", true)))

        DenseUnionVector(allocator, "op", listOf(putLeg, NullVector("delete"), NullVector("erase"))).use { dest ->
            dest.vectorFor("put").writeObject(mapOf("a" to 1))

            putLessOpDuv().use { putLess ->
                putLess.vectorFor("delete").writeNull()
                putLess.rowCopier(dest).copyRange(0, putLess.valueCount)

                assertEquals(2, dest.valueCount)
                assertEquals(TaggedValue("put".kw, mapOf("a" to 1)), dest.getObject(0))
                assertFalse(putLeg.nullable)
            }
        }
    }

    @Test
    fun `copy whole DUV into empty DUV`() {
        DenseUnionVector(
            allocator, "src",
            listOf(
                IntVector.open(allocator, "i32", false),
                Utf8Vector(allocator, "utf8", true)
            )
        ).use { src ->
            src.vectorFor("i32").writeInt(12)
            src.vectorFor("utf8").writeObject("hello")
            src.vectorFor("i32").writeInt(34)
            src.vectorFor("utf8").writeNull()

            DenseUnionVector(
                allocator, "dest",
                listOf(
                    IntVector.open(allocator, "i32", false),
                    Utf8Vector(allocator, "utf8", true)
                )
            ).use { dest ->
                src.rowCopier(dest).copyRange(0, src.valueCount)

                assertEquals(src.valueCount, dest.valueCount)
                assertEquals(src.asList, dest.asList)
                assertEquals(src.typeIds(), dest.typeIds())
                assertEquals(src.offsets(), dest.offsets())
            }
        }
    }

    @Test
    fun `copy whole DUV into DUV with existing rows`() {
        DenseUnionVector(
            allocator, "src",
            listOf(
                IntVector.open(allocator, "i32", false),
                Utf8Vector(allocator, "utf8", true)
            )
        ).use { src ->
            src.vectorFor("i32").writeInt(12)
            src.vectorFor("utf8").writeObject("hello")
            src.vectorFor("i32").writeInt(34)

            DenseUnionVector(
                allocator, "dest",
                listOf(
                    IntVector.open(allocator, "i32", false),
                    Utf8Vector(allocator, "utf8", true)
                )
            ).use { dest ->
                dest.vectorFor("utf8").writeObject("existing")
                dest.vectorFor("i32").writeInt(99)

                assertEquals(2, dest.valueCount)

                src.rowCopier(dest).copyRange(0, src.valueCount)

                assertEquals(5, dest.valueCount)
                assertEquals(listOf("existing", 99, 12, "hello", 34), dest.asList)
                assertEquals(listOf(1, 0, 0, 1, 0).map { it.toByte() }, dest.typeIds())
                assertEquals(listOf(0, 0, 1, 1, 2), dest.offsets())
            }
        }
    }
}