package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.ofType
import kotlin.use

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
}
