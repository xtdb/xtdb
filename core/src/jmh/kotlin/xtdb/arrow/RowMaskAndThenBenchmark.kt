package xtdb.arrow

import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit
import kotlin.random.Random

private const val ROWS = 1 shl 16

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(ROWS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
open class RowMaskAndThenBenchmark {

    @Param("0.02:0.5", "0.5:0.5", "0.98:0.5", "0.02:0.98")
    lateinit var densities: String

    private lateinit var mask: RowMask
    private lateinit var refinement: RowMask

    private fun randomMask(rowCount: Int, density: Double, rnd: Random): RowMask {
        val builder = RowMask.Builder(rowCount)
        for (row in 0 until rowCount) if (rnd.nextDouble() < density) builder.add(row)
        return builder.build()
    }

    @Setup
    fun setup() {
        val (maskDensity, refinementDensity) = densities.split(':').map(String::toDouble)
        val rnd = Random(42)
        mask = randomMask(ROWS, maskDensity, rnd)
        refinement = randomMask(mask.cardinality, refinementDensity, rnd)
    }

    @Benchmark
    fun andThen(): RowMask = mask.andThen(refinement)
}
