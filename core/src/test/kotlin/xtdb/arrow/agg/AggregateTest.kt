package xtdb.arrow.agg

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.*
import xtdb.arrow.VectorType.Companion.I32
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
        IntVector.open(allocator, "values", false).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeInt(10)
                valuesVec.writeInt(20)
                valuesVec.writeInt(30)
                valuesVec.writeInt(40)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(1)
                groupMapping.writeInt(1)

                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val sumFactory = Sum("values", "sum", I32.arrowType, false)
                val sumAgg = sumFactory.build(allocator)

                sumAgg.aggregate(inRel, groupMapping)
                sumAgg.openFinishedVector().use { result ->
                    assertEquals(2, result.valueCount)
                    assertEquals(30, result.getInt(0))
                    assertEquals(70, result.getInt(1))
                }

                sumAgg.close()
            }
        }
    }

    @Test
    fun `Sum handles nulls`() {
        IntVector.open(allocator, "values", true).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeInt(10)
                valuesVec.writeNull()
                valuesVec.writeInt(30)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(0)

                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val sumFactory = Sum("values", "sum", I32.arrowType, false)
                val sumAgg = sumFactory.build(allocator)

                sumAgg.aggregate(inRel, groupMapping)
                sumAgg.openFinishedVector().use { result ->
                    assertEquals(1, result.valueCount)
                    assertEquals(40, result.getInt(0))
                }

                sumAgg.close()
            }
        }
    }

    @Test
    fun `Average calculates mean of values`() {
        DoubleVector(allocator, "values", false).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeDouble(10.0)
                valuesVec.writeDouble(20.0)
                valuesVec.writeDouble(30.0)
                valuesVec.writeDouble(40.0)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(1)
                groupMapping.writeInt(1)

                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val avgFactory = Average("values", "avg", false)
                val avgAgg = avgFactory.build(allocator)

                avgAgg.aggregate(inRel, groupMapping)
                avgAgg.openFinishedVector().use { result ->
                    assertEquals(2, result.valueCount)
                    assertEquals(15.0, result.getDouble(0), 0.0001)
                    assertEquals(35.0, result.getDouble(1), 0.0001)
                }

                avgAgg.close()
            }
        }
    }

    @Test
    fun `Variance population calculates variance`() {
        DoubleVector(allocator, "values", false).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeDouble(10.0)
                valuesVec.writeDouble(20.0)
                valuesVec.writeDouble(30.0)
                valuesVec.writeDouble(40.0)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(1)
                groupMapping.writeInt(1)

                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val varFactory = VariancePop("values", "var", false)
                val varAgg = varFactory.build(allocator)

                varAgg.aggregate(inRel, groupMapping)
                varAgg.openFinishedVector().use { result ->
                    assertEquals(2, result.valueCount)
                    assertEquals(25.0, result.getDouble(0), 0.0001)
                    assertEquals(25.0, result.getDouble(1), 0.0001)
                }

                varAgg.close()
            }
        }
    }

    @Test
    fun `Variance sample calculates sample variance`() {
        DoubleVector(allocator, "values", false).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeDouble(10.0)
                valuesVec.writeDouble(20.0)
                valuesVec.writeDouble(30.0)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(0)

                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val varFactory = VarianceSamp("values", "var", false)
                val varAgg = varFactory.build(allocator)

                varAgg.aggregate(inRel, groupMapping)
                varAgg.openFinishedVector().use { result ->
                    assertEquals(1, result.valueCount)
                    assertEquals(100.0, result.getDouble(0), 0.0001)
                }

                varAgg.close()
            }
        }
    }

    @Test
    fun `Stddev population calculates standard deviation`() {
        DoubleVector(allocator, "values", false).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeDouble(10.0)
                valuesVec.writeDouble(20.0)
                valuesVec.writeDouble(30.0)
                valuesVec.writeDouble(40.0)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(1)
                groupMapping.writeInt(1)

                val inRel = RelationReader.from(listOf(valuesVec), 4)
                val stddevFactory = StdDevPop("values", "stddev", false)
                val stddevAgg = stddevFactory.build(allocator)

                stddevAgg.aggregate(inRel, groupMapping)
                stddevAgg.openFinishedVector().use { result ->
                    assertEquals(2, result.valueCount)
                    assertEquals(5.0, result.getDouble(0), 0.0001)
                    assertEquals(5.0, result.getDouble(1), 0.0001)
                }

                stddevAgg.close()
            }
        }
    }

    @Test
    fun `Stddev sample calculates sample standard deviation`() {
        DoubleVector(allocator, "values", false).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeDouble(10.0)
                valuesVec.writeDouble(20.0)
                valuesVec.writeDouble(30.0)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(0)

                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val stddevFactory = StdDevSamp("values", "stddev", false)
                val stddevAgg = stddevFactory.build(allocator)

                stddevAgg.aggregate(inRel, groupMapping)
                stddevAgg.openFinishedVector().use { result ->
                    assertEquals(1, result.valueCount)
                    assertEquals(10.0, result.getDouble(0), 0.0001)
                }

                stddevAgg.close()
            }
        }
    }

    @Test
    fun `Variance handles nulls correctly`() {
        DoubleVector(allocator, "values", true).use { valuesVec ->
            IntVector.open(allocator, "group-mapping", false).use { groupMapping ->
                valuesVec.writeDouble(10.0)
                valuesVec.writeNull()
                valuesVec.writeDouble(30.0)

                groupMapping.writeInt(0)
                groupMapping.writeInt(0)
                groupMapping.writeInt(0)

                val inRel = RelationReader.from(listOf(valuesVec), 3)
                val varFactory = VariancePop("values", "var", false)
                val varAgg = varFactory.build(allocator)

                varAgg.aggregate(inRel, groupMapping)
                varAgg.openFinishedVector().use { result ->
                    assertEquals(1, result.valueCount)
                    assertEquals(100.0, result.getDouble(0), 0.0001)
                }

                varAgg.close()
            }
        }
    }
}
