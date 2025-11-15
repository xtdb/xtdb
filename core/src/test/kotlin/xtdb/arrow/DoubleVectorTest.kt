package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DoubleVectorTest {
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
    fun `divideInto divides doubles by longs`() {
        DoubleVector(allocator, "dividend", false).use { dividendVec ->
            LongVector(allocator, "divisor", false).use { divisorVec ->
                DoubleVector(allocator, "result", false).use { resultVec ->
                    dividendVec.writeDouble(100.0)
                    dividendVec.writeDouble(50.0)
                    dividendVec.writeDouble(75.0)

                    divisorVec.writeLong(4)
                    divisorVec.writeLong(2)
                    divisorVec.writeLong(3)

                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(3, resultVec.valueCount)
                    assertEquals(25.0, resultVec.getDouble(0), 0.0001)
                    assertEquals(25.0, resultVec.getDouble(1), 0.0001)
                    assertEquals(25.0, resultVec.getDouble(2), 0.0001)
                }
            }
        }
    }

    @Test
    fun `divideInto handles null dividend`() {
        DoubleVector(allocator, "dividend", true).use { dividendVec ->
            LongVector(allocator, "divisor", false).use { divisorVec ->
                DoubleVector(allocator, "result", true).use { resultVec ->
                    dividendVec.writeNull()
                    dividendVec.writeDouble(50.0)

                    divisorVec.writeLong(4)
                    divisorVec.writeLong(2)

                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(2, resultVec.valueCount)
                    assertTrue(resultVec.isNull(0))
                    assertEquals(25.0, resultVec.getDouble(1), 0.0001)
                }
            }
        }
    }

    @Test
    fun `divideInto handles null divisor`() {
        DoubleVector(allocator, "dividend", false).use { dividendVec ->
            LongVector(allocator, "divisor", true).use { divisorVec ->
                DoubleVector(allocator, "result", true).use { resultVec ->
                    dividendVec.writeDouble(100.0)
                    dividendVec.writeDouble(50.0)

                    divisorVec.writeLong(4)
                    divisorVec.writeNull()

                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(2, resultVec.valueCount)
                    assertEquals(25.0, resultVec.getDouble(0), 0.0001)
                    assertTrue(resultVec.isNull(1))
                }
            }
        }
    }

    @Test
    fun `squareInto squares values`() {
        DoubleVector(allocator, "input", false).use { inVec ->
            DoubleVector(allocator, "result", false).use { resultVec ->
                inVec.writeDouble(2.0)
                inVec.writeDouble(3.0)
                inVec.writeDouble(5.0)

                inVec.squareInto(resultVec)

                assertEquals(3, resultVec.valueCount)
                assertEquals(4.0, resultVec.getDouble(0), 0.0001)
                assertEquals(9.0, resultVec.getDouble(1), 0.0001)
                assertEquals(25.0, resultVec.getDouble(2), 0.0001)
            }
        }
    }

    @Test
    fun `squareInto handles nulls`() {
        DoubleVector(allocator, "input", true).use { inVec ->
            DoubleVector(allocator, "result", true).use { resultVec ->
                inVec.writeDouble(2.0)
                inVec.writeNull()
                inVec.writeDouble(5.0)

                inVec.squareInto(resultVec)

                assertEquals(3, resultVec.valueCount)
                assertEquals(4.0, resultVec.getDouble(0), 0.0001)
                assertTrue(resultVec.isNull(1))
                assertEquals(25.0, resultVec.getDouble(2), 0.0001)
            }
        }
    }

    @Test
    fun `sqrtInto takes square root of values`() {
        DoubleVector(allocator, "input", false).use { inVec ->
            DoubleVector(allocator, "result", false).use { resultVec ->
                inVec.writeDouble(4.0)
                inVec.writeDouble(9.0)
                inVec.writeDouble(25.0)

                inVec.sqrtInto(resultVec)

                assertEquals(3, resultVec.valueCount)
                assertEquals(2.0, resultVec.getDouble(0), 0.0001)
                assertEquals(3.0, resultVec.getDouble(1), 0.0001)
                assertEquals(5.0, resultVec.getDouble(2), 0.0001)
            }
        }
    }

    @Test
    fun `sqrtInto handles nulls`() {
        DoubleVector(allocator, "input", true).use { inVec ->
            DoubleVector(allocator, "result", true).use { resultVec ->
                inVec.writeDouble(4.0)
                inVec.writeNull()
                inVec.writeDouble(25.0)

                inVec.sqrtInto(resultVec)

                assertEquals(3, resultVec.valueCount)
                assertEquals(2.0, resultVec.getDouble(0), 0.0001)
                assertTrue(resultVec.isNull(1))
                assertEquals(5.0, resultVec.getDouble(2), 0.0001)
            }
        }
    }
}
