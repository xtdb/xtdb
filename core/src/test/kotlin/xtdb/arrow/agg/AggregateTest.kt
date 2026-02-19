package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.*
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.arrow.VectorType.Companion.scalar
import java.math.BigDecimal
import kotlin.use

class AggregateTest {
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
    fun `Sum aggregates integer values`() {
        Vector.fromList(allocator, "values", I32, listOf(10, 20, 30, 40)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 1, 1)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val sumFactory = Sum("values", "sum", I32, false)
                sumFactory.build(allocator, RelationReader.DUAL).use { sumAgg ->
                    sumAgg.aggregate(inRel, groupMapping)
                    sumAgg.openFinishedVector().use { result ->
                        assertEquals(2, result.valueCount)
                        assertEquals(30, result.getInt(0))
                        assertEquals(70, result.getInt(1))
                    }
                }
            }
        }
    }

    @Test
    fun `Sum handles nulls`() {
        Vector.fromList(allocator, "values", maybe(I32), listOf(10, null, 30)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 0)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val sumFactory = Sum("values", "sum", I32, false)
                sumFactory.build(allocator, RelationReader.DUAL).use { sumAgg ->
                    sumAgg.aggregate(inRel, groupMapping)
                    sumAgg.openFinishedVector().use { result ->
                        assertEquals(1, result.valueCount)
                        assertEquals(40, result.getInt(0))
                    }
                }
            }
        }
    }

    @Test
    fun `Average calculates mean of values`() {
        Vector.fromList(allocator, "values", F64, listOf(10.0, 20.0, 30.0, 40.0)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 1, 1)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val avgFactory = Average("values", valuesVec.type, "avg", false)
                avgFactory.build(allocator, RelationReader.DUAL).use { avgAgg ->
                    avgAgg.aggregate(inRel, groupMapping)
                    avgAgg.openFinishedVector().use { result ->
                        assertEquals(2, result.valueCount)
                        assertEquals(15.0, result.getDouble(0), 0.0001)
                        assertEquals(35.0, result.getDouble(1), 0.0001)
                    }
                }
            }
        }
    }

    @Test
    fun `Variance population calculates variance`() {
        Vector.fromList(allocator, "values", F64, listOf(10.0, 20.0, 30.0, 40.0)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 1, 1)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val varFactory = VariancePop("values", "var", false)
                varFactory.build(allocator, RelationReader.DUAL).use { varAgg ->
                    varAgg.aggregate(inRel, groupMapping)
                    varAgg.openFinishedVector().use { result ->
                        assertEquals(2, result.valueCount)
                        assertEquals(25.0, result.getDouble(0), 0.0001)
                        assertEquals(25.0, result.getDouble(1), 0.0001)
                    }
                }
            }
        }
    }

    @Test
    fun `Variance sample calculates sample variance`() {
        Vector.fromList(allocator, "values", F64, listOf(10.0, 20.0, 30.0)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 0)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val varFactory = VarianceSamp("values", "var", false)
                varFactory.build(allocator, RelationReader.DUAL).use { varAgg ->
                    varAgg.aggregate(inRel, groupMapping)
                    varAgg.openFinishedVector().use { result ->
                        assertEquals(1, result.valueCount)
                        assertEquals(100.0, result.getDouble(0), 0.0001)
                    }
                }
            }
        }
    }

    @Test
    fun `StdDev population calculates standard deviation`() {
        Vector.fromList(allocator, "values", F64, listOf(10.0, 20.0, 30.0, 40.0)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 1, 1)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val stdDevFactory = StdDevPop("values", "std-dev", false)
                stdDevFactory.build(allocator, RelationReader.DUAL).use { stdDevAgg ->
                    stdDevAgg.aggregate(inRel, groupMapping)
                    stdDevAgg.openFinishedVector().use { result ->
                        assertEquals(2, result.valueCount)
                        assertEquals(5.0, result.getDouble(0), 0.0001)
                        assertEquals(5.0, result.getDouble(1), 0.0001)
                    }
                }
            }
        }
    }

    @Test
    fun `StdDev sample calculates sample standard deviation`() {
        Vector.fromList(allocator, "values", F64, listOf(10.0, 20.0, 30.0)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 0)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val stdDevFactory = StdDevSamp("values", "std-dev", false)
                stdDevFactory.build(allocator, RelationReader.DUAL).use { stdDevAgg ->
                    stdDevAgg.aggregate(inRel, groupMapping)
                    stdDevAgg.openFinishedVector().use { result ->
                        assertEquals(1, result.valueCount)
                        assertEquals(10.0, result.getDouble(0), 0.0001)
                    }
                }
            }
        }
    }

    @Test
    fun `Variance handles nulls correctly`() {
        Vector.fromList(allocator, "values", maybe(F64), listOf(10.0, null, 30.0)).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 0)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val varFactory = VariancePop("values", "var", false)
                varFactory.build(allocator, RelationReader.DUAL).use { varAgg ->
                    varAgg.aggregate(inRel, groupMapping)
                    varAgg.openFinishedVector().use { result ->
                        assertEquals(1, result.valueCount)
                        assertEquals(100.0, result.getDouble(0), 0.0001)
                    }
                }
            }
        }
    }

    @Test
    fun `Sum aggregates decimal values`() {
        val decimalType = scalar(ArrowType.Decimal(10, 2, 128))
        Vector.fromList(allocator, "values", decimalType,
            listOf(BigDecimal("10.50"), BigDecimal("20.25"), BigDecimal("30.75"), BigDecimal("40.00"))).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 1, 1)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val sumFactory = Sum("values", "sum", decimalType, false)
                sumFactory.build(allocator, RelationReader.DUAL).use { sumAgg ->
                    sumAgg.aggregate(inRel, groupMapping)
                    sumAgg.openFinishedVector().use { result ->
                        assertEquals(2, result.valueCount)
                        assertEquals(BigDecimal("30.75"), result.getObject(0))
                        assertEquals(BigDecimal("70.75"), result.getObject(1))
                    }
                }
            }
        }
    }

    @Test
    fun `Average calculates mean of decimal values`() {
        val decimalType = scalar(ArrowType.Decimal(10, 2, 128))
        Vector.fromList(allocator, "values", decimalType,
            listOf(BigDecimal("10.00"), BigDecimal("20.00"), BigDecimal("30.00"), BigDecimal("40.00"))).use { valuesVec ->
            Vector.fromList(allocator, "group-mapping", I32, listOf(0, 0, 1, 1)).use { groupMapping ->
                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val avgFactory = Average("values", valuesVec.type, "avg", false)
                avgFactory.build(allocator, RelationReader.DUAL).use { avgAgg ->
                    avgAgg.aggregate(inRel, groupMapping)
                    avgAgg.openFinishedVector().use { result ->
                        assertEquals(2, result.valueCount)
                        assertEquals(15.0, result.getDouble(0), 0.0001)
                        assertEquals(35.0, result.getDouble(1), 0.0001)
                    }
                }
            }
        }
    }
}
