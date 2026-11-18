package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit
import kotlin.random.Random

private const val ROWS = 1 shl 20
private const val INNER_ROWS = ROWS / 4 * 5

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(ROWS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
open class VectorReadBenchmark {
    private lateinit var al: BufferAllocator
    private lateinit var vecs: List<LongVector>

    private lateinit var direct: VectorReader
    private lateinit var directNullable: VectorReader
    private lateinit var selection: VectorReader
    private lateinit var selectionNullable: VectorReader
    private lateinit var slice: VectorReader
    private lateinit var sliceNullable: VectorReader

    private fun longVector(rows: Int, nullable: Boolean, rnd: Random) =
        LongVector(al, "v", nullable).apply {
            repeat(rows) { if (nullable && rnd.nextInt(10) == 0) writeNull() else writeLong(rnd.nextLong()) }
        }

    @Setup
    fun setup() {
        al = RootAllocator()
        val rnd = Random(0)

        val flat = longVector(ROWS, false, rnd)
        val flatNullable = longVector(ROWS, true, rnd)
        val inner = longVector(INNER_ROWS, false, rnd)
        val innerNullable = longVector(INNER_ROWS, true, rnd)
        vecs = listOf(flat, flatNullable, inner, innerNullable)

        val idxs = (0 until INNER_ROWS).shuffled(rnd).take(ROWS).sorted().toIntArray()
        val sliceStart = (INNER_ROWS - ROWS) / 2

        direct = flat
        directNullable = flatNullable
        selection = inner.select(idxs)
        selectionNullable = innerNullable.select(idxs)
        slice = inner.select(sliceStart, ROWS)
        sliceNullable = innerNullable.select(sliceStart, ROWS)
    }

    @TearDown
    fun tearDown() {
        vecs.forEach { it.close() }
        al.close()
    }

    private fun sum(v: VectorReader): Long {
        var acc = 0L
        for (idx in 0 until v.valueCount) acc += v.getLong(idx)
        return acc
    }

    private fun sumNullable(v: VectorReader): Long {
        var acc = 0L
        for (idx in 0 until v.valueCount) if (!v.isNull(idx)) acc += v.getLong(idx)
        return acc
    }

    @Benchmark
    fun direct() = sum(direct)

    @Benchmark
    fun directNullable() = sumNullable(directNullable)

    @Benchmark
    fun selection() = sum(selection)

    @Benchmark
    fun selectionNullable() = sumNullable(selectionNullable)

    @Benchmark
    fun slice() = sum(slice)

    @Benchmark
    fun sliceNullable() = sumNullable(sliceNullable)
}
