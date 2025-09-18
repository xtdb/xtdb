package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class FixedWidthVectorTest {
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
    fun testIntVector() {
        IntVector.open(allocator,"foo", false).use { vector ->
            vector.writeInt(42)
            vector.writeInt(43)
            vector.writeInt(44)

            assertEquals(3, vector.valueCount)

            assertEquals(42, vector.getInt(0))
            assertEquals(43, vector.getInt(1))
            assertEquals(44, vector.getInt(2))
        }
    }

    @Test
    fun `from null into Int vector`() {
        NullVector("v1" ).use { nullVector ->
            nullVector.writeNull()
            nullVector.writeNull()
            nullVector.writeNull()

            IntVector.open(allocator, "v2", true).use { copy ->
                val copier = nullVector.rowCopier(copy)
                copier.copyRow(0)
                copier.copyRow(1)
                copier.copyRow(2)

                assertEquals(3, copy.valueCount)
                assertNull(copy.getObject(1))
            }
        }
    }



}