package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType

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
        Vector.fromList(allocator, "dividend" ofType F64, listOf(100.0, 50.0, 75.0)).use { dividendVec ->
            Vector.fromList(allocator, "divisor" ofType I32, listOf(4, 2, 3)).use { divisorVec ->
                Vector.open(allocator, "res" ofType F64).use { resultVec ->
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
        Vector.fromList(allocator, "dividend" ofType maybe(F64), listOf(null, 50.0)).use { dividendVec ->
            Vector.fromList(allocator, "divisor" ofType I32, listOf(4, 2)).use { divisorVec ->
                Vector.open(allocator, "result" ofType maybe(F64)).use { resultVec ->
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
        Vector.fromList(allocator, "dividend" ofType F64, listOf(100.0, 50.0)).use { dividendVec ->
            Vector.fromList(allocator, "divisor" ofType maybe(I32), listOf(4, null)).use { divisorVec ->
                Vector.open(allocator, "result" ofType maybe(F64)).use { resultVec ->
                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(2, resultVec.valueCount)
                    assertEquals(25.0, resultVec.getDouble(0), 0.0001)
                    assertTrue(resultVec.isNull(1))
                }
            }
        }
    }

    @Test
    fun `divideInto handles division by zero`() {
        Vector.fromList(allocator, "dividend" ofType F64, listOf(100.0, 50.0, 75.0)).use { dividendVec ->
            Vector.fromList(allocator, "divisor" ofType I32, listOf(0, 2, 0)).use { divisorVec ->
                Vector.open(allocator, "result" ofType maybe(F64)).use { resultVec ->
                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(3, resultVec.valueCount)
                    assertTrue(resultVec.isNull(0))
                    assertEquals(25.0, resultVec.getDouble(1), 0.0001)
                    assertTrue(resultVec.isNull(2))
                }
            }
        }
    }

    @Test
    fun `squareInto squares values`() {
        Vector.fromList(allocator, "input" ofType F64, listOf(2.0, 3.0, 5.0)).use { inVec ->
            Vector.open(allocator, "result" ofType F64).use { resultVec ->
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
        Vector.fromList(allocator, "input" ofType maybe(F64), listOf(2.0, null, 5.0)).use { inVec ->
            Vector.open(allocator, "result" ofType maybe(F64)).use { resultVec ->
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
        Vector.fromList(allocator, "input" ofType F64, listOf(4.0, 9.0, 25.0)).use { inVec ->
            Vector.open(allocator, "result" ofType F64).use { resultVec ->
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
        Vector.fromList(allocator, "input" ofType maybe(F64), listOf(4.0, null, 25.0)).use { inVec ->
            Vector.open(allocator, "result" ofType maybe(F64)).use { resultVec ->
                inVec.sqrtInto(resultVec)

                assertEquals(3, resultVec.valueCount)
                assertEquals(2.0, resultVec.getDouble(0), 0.0001)
                assertTrue(resultVec.isNull(1))
                assertEquals(5.0, resultVec.getDouble(2), 0.0001)
            }
        }
    }
}
