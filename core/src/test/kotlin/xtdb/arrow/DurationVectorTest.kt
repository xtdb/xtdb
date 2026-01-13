package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.DURATION_MICRO
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.ofType
import java.time.Duration

class DurationVectorTest {
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
    fun `divideInto divides durations by integers`() {
        Vector.fromList(allocator, "dividend", DURATION_MICRO,
            listOf(Duration.ofSeconds(100), Duration.ofSeconds(50), Duration.ofSeconds(75))).use { dividendVec ->
            Vector.fromList(allocator, "divisor", I32, listOf(4, 2, 3)).use { divisorVec ->
                allocator.openVector("result", DURATION_MICRO).use { resultVec ->
                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(3, resultVec.valueCount)
                    assertEquals(Duration.ofSeconds(25), resultVec.getObject(0))
                    assertEquals(Duration.ofSeconds(25), resultVec.getObject(1))
                    assertEquals(Duration.ofSeconds(25), resultVec.getObject(2))
                }
            }
        }
    }

    @Test
    fun `divideInto handles null dividend`() {
        Vector.fromList(allocator, "dividend", maybe(DURATION_MICRO),
            listOf(null, Duration.ofSeconds(50))).use { dividendVec ->
            Vector.fromList(allocator, "divisor", I32, listOf(4, 2)).use { divisorVec ->
                allocator.openVector("result", maybe(DURATION_MICRO)).use { resultVec ->
                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(2, resultVec.valueCount)
                    assertTrue(resultVec.isNull(0))
                    assertEquals(Duration.ofSeconds(25), resultVec.getObject(1))
                }
            }
        }
    }

    @Test
    fun `divideInto handles null divisor`() {
        Vector.fromList(allocator, "dividend", DURATION_MICRO,
            listOf(Duration.ofSeconds(100), Duration.ofSeconds(50))).use { dividendVec ->
            Vector.fromList(allocator, "divisor", maybe(I32), listOf(4, null)).use { divisorVec ->
                allocator.openVector("result", maybe(DURATION_MICRO)).use { resultVec ->
                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(2, resultVec.valueCount)
                    assertEquals(Duration.ofSeconds(25), resultVec.getObject(0))
                    assertTrue(resultVec.isNull(1))
                }
            }
        }
    }

    @Test
    fun `divideInto handles division by zero`() {
        Vector.fromList(allocator, "dividend", DURATION_MICRO,
            listOf(Duration.ofSeconds(100), Duration.ofSeconds(50), Duration.ofSeconds(75))).use { dividendVec ->
            Vector.fromList(allocator, "divisor", I32, listOf(0, 2, 0)).use { divisorVec ->
                allocator.openVector("result", maybe(DURATION_MICRO)).use { resultVec ->
                    dividendVec.divideInto(divisorVec, resultVec)

                    assertEquals(3, resultVec.valueCount)
                    assertTrue(resultVec.isNull(0))
                    assertEquals(Duration.ofSeconds(25), resultVec.getObject(1))
                    assertTrue(resultVec.isNull(2))
                }
            }
        }
    }
}
