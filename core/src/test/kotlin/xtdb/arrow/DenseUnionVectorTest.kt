package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
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
            val i32Leg = duv.legWriter("i32")
            val utf8Leg = duv.legWriter("utf8")

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
}