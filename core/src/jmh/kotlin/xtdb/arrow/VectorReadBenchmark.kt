package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.TimeUnit.MICROSECOND
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit
import kotlin.random.Random

private const val ROWS = 1 shl 20
private const val INNER_ROWS = ROWS / 4 * 5
private const val MEGA_ROWS = ROWS / 4
private const val MEGA_INNER_ROWS = MEGA_ROWS / 4 * 5
private const val SPARSE_ROWS = ROWS / 16
private const val SPARSE_INNER_ROWS = SPARSE_ROWS * 50

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(ROWS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
open class VectorReadBenchmark {
    private lateinit var al: BufferAllocator
    private lateinit var vecs: List<Vector>

    private lateinit var direct: VectorReader
    private lateinit var directNullable: VectorReader
    private lateinit var selection: VectorReader
    private lateinit var selectionNullable: VectorReader
    private lateinit var slice: VectorReader
    private lateinit var sliceNullable: VectorReader
    private lateinit var masked: VectorReader
    private lateinit var maskedNullable: VectorReader
    private lateinit var sparseSelectionNullable: VectorReader
    private lateinit var sparseMaskedNullable: VectorReader

    private lateinit var utf8Direct: VectorReader
    private lateinit var utf8DirectNullable: VectorReader
    private lateinit var utf8Selection: VectorReader
    private lateinit var utf8SelectionNullable: VectorReader

    private lateinit var megaDirect: List<VectorReader>
    private lateinit var megaSelection: List<VectorReader>
    private lateinit var megaSlice: List<VectorReader>

    private fun longVector(rows: Int, nullable: Boolean, rnd: Random) =
        LongVector(al, "v", nullable).apply {
            repeat(rows) { if (nullable && rnd.nextInt(10) == 0) writeNull() else writeLong(rnd.nextLong()) }
        }

    private fun utf8Vector(rows: Int, nullable: Boolean, rnd: Random) =
        Utf8Vector(al, "v", nullable).apply {
            repeat(rows) {
                if (nullable && rnd.nextInt(10) == 0) writeNull()
                else writeBytes(ByteArray(1 + rnd.nextInt(16)) { 'a'.code.toByte() })
            }
        }

    private fun megaVectors(rows: Int, rnd: Random): List<Vector> {
        fun Vector.fill(write: Vector.() -> Unit) = apply {
            repeat(rows) { if (rnd.nextInt(10) == 0) writeNull() else write() }
        }

        return listOf(
            LongVector(al, "v", true).fill { writeLong(rnd.nextLong()) },
            TimestampLocalVector(al, "v", true, MICROSECOND).fill { writeLong(rnd.nextLong()) },
            DurationVector(al, "v", true, MICROSECOND).fill { writeLong(rnd.nextLong()) },
            DateMilliVector(al, "v", true).fill { writeLong(rnd.nextLong()) },
        )
    }

    private fun rowMask(idxs: IntArray, rowCount: Int): RowMask {
        val words = LongArray((rowCount + 63) ushr 6)
        for (idx in idxs) words[idx ushr 6] = words[idx ushr 6] or (1L shl (idx and 63))
        return RowMask.ofWords(words, rowCount)
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

        val utf8Flat = utf8Vector(ROWS, false, rnd)
        val utf8FlatNullable = utf8Vector(ROWS, true, rnd)
        val utf8Inner = utf8Vector(INNER_ROWS, false, rnd)
        val utf8InnerNullable = utf8Vector(INNER_ROWS, true, rnd)
        vecs += listOf(utf8Flat, utf8FlatNullable, utf8Inner, utf8InnerNullable)

        utf8Direct = utf8Flat
        utf8DirectNullable = utf8FlatNullable
        utf8Selection = utf8Inner.select(idxs)
        utf8SelectionNullable = utf8InnerNullable.select(idxs)

        val mask = rowMask(idxs, INNER_ROWS)
        masked = MaskedVector(inner, mask)
        maskedNullable = MaskedVector(innerNullable, mask)

        val sparseInner = longVector(SPARSE_INNER_ROWS, true, rnd)
        vecs += sparseInner
        val sparseIdxs = (0 until SPARSE_INNER_ROWS).shuffled(rnd).take(SPARSE_ROWS).sorted().toIntArray()
        sparseSelectionNullable = sparseInner.select(sparseIdxs)
        sparseMaskedNullable = MaskedVector(sparseInner, rowMask(sparseIdxs, SPARSE_INNER_ROWS))

        val megaFlat = megaVectors(MEGA_ROWS, rnd)
        val megaInner = megaVectors(MEGA_INNER_ROWS, rnd)
        vecs += megaFlat + megaInner

        val megaIdxs = (0 until MEGA_INNER_ROWS).shuffled(rnd).take(MEGA_ROWS).sorted().toIntArray()
        val megaSliceStart = (MEGA_INNER_ROWS - MEGA_ROWS) / 2

        megaDirect = megaFlat
        megaSelection = megaInner.map { it.select(megaIdxs) }
        megaSlice = megaInner.map { it.select(megaSliceStart, MEGA_ROWS) }
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

    private fun sumReader(v: VectorReader): Long {
        var acc = 0L
        val rows = v.valueReader()
        while (rows.hasNext()) acc += rows.next().readLong()
        return acc
    }

    private fun sumNullableReader(v: VectorReader): Long {
        var acc = 0L
        val rows = v.valueReader()
        while (rows.hasNext()) {
            val row = rows.next()
            if (!row.isNull) acc += row.readLong()
        }
        return acc
    }

    @Benchmark
    fun readerDirect() = sumReader(direct)

    @Benchmark
    fun readerDirectNullable() = sumNullableReader(directNullable)

    @Benchmark
    fun readerSelection() = sumReader(selection)

    @Benchmark
    fun readerSelectionNullable() = sumNullableReader(selectionNullable)

    @Benchmark
    fun readerSlice() = sumReader(slice)

    @Benchmark
    fun readerSliceNullable() = sumNullableReader(sliceNullable)

    @Benchmark
    fun masked() = sum(masked)

    @Benchmark
    fun maskedNullable() = sumNullable(maskedNullable)

    @Benchmark
    fun readerMasked() = sumReader(masked)

    @Benchmark
    fun readerMaskedNullable() = sumNullableReader(maskedNullable)

    @Benchmark
    @OperationsPerInvocation(SPARSE_ROWS)
    fun sparseSelectionNullable() = sumNullable(sparseSelectionNullable)

    @Benchmark
    @OperationsPerInvocation(SPARSE_ROWS)
    fun sparseMaskedNullable() = sumNullable(sparseMaskedNullable)

    @Benchmark
    @OperationsPerInvocation(SPARSE_ROWS)
    fun readerSparseSelectionNullable() = sumNullableReader(sparseSelectionNullable)

    @Benchmark
    @OperationsPerInvocation(SPARSE_ROWS)
    fun readerSparseMaskedNullable() = sumNullableReader(sparseMaskedNullable)

    @Benchmark
    fun megaDirect() = megaDirect.sumOf { sumNullable(it) }

    @Benchmark
    fun megaSelection() = megaSelection.sumOf { sumNullable(it) }

    @Benchmark
    fun megaSlice() = megaSlice.sumOf { sumNullable(it) }

    @Benchmark
    fun megaReaderDirect() = megaDirect.sumOf { sumNullableReader(it) }

    @Benchmark
    fun megaReaderSelection() = megaSelection.sumOf { sumNullableReader(it) }

    @Benchmark
    fun megaReaderSlice() = megaSlice.sumOf { sumNullableReader(it) }

    private fun countNulls(v: VectorReader): Long {
        var acc = 0L
        for (idx in 0 until v.valueCount) if (v.isNull(idx)) acc++
        return acc
    }

    private fun countNullsReader(v: VectorReader): Long {
        var acc = 0L
        val rows = v.valueReader()
        while (rows.hasNext()) if (rows.next().isNull) acc++
        return acc
    }

    private fun sumBytes(v: VectorReader): Long {
        var acc = 0L
        for (idx in 0 until v.valueCount) if (!v.isNull(idx)) {
            val buf = v.getBytes(idx)
            acc += buf.remaining() + buf.get(0)
        }
        return acc
    }

    private fun sumBytesReader(v: VectorReader): Long {
        var acc = 0L
        val rows = v.valueReader()
        while (rows.hasNext()) {
            val row = rows.next()
            if (!row.isNull) {
                val buf = row.readBytes()
                acc += buf.remaining() + buf.get(0)
            }
        }
        return acc
    }

    @Benchmark
    fun utf8IsNullDirect() = countNulls(utf8Direct)

    @Benchmark
    fun utf8IsNullDirectNullable() = countNulls(utf8DirectNullable)

    @Benchmark
    fun utf8IsNullSelection() = countNulls(utf8Selection)

    @Benchmark
    fun utf8IsNullSelectionNullable() = countNulls(utf8SelectionNullable)

    @Benchmark
    fun utf8ReaderIsNullDirect() = countNullsReader(utf8Direct)

    @Benchmark
    fun utf8ReaderIsNullDirectNullable() = countNullsReader(utf8DirectNullable)

    @Benchmark
    fun utf8ReaderIsNullSelection() = countNullsReader(utf8Selection)

    @Benchmark
    fun utf8ReaderIsNullSelectionNullable() = countNullsReader(utf8SelectionNullable)

    @Benchmark
    fun utf8BytesDirect() = sumBytes(utf8Direct)

    @Benchmark
    fun utf8BytesDirectNullable() = sumBytes(utf8DirectNullable)

    @Benchmark
    fun utf8BytesSelection() = sumBytes(utf8Selection)

    @Benchmark
    fun utf8BytesSelectionNullable() = sumBytes(utf8SelectionNullable)

    @Benchmark
    fun utf8ReaderBytesDirect() = sumBytesReader(utf8Direct)

    @Benchmark
    fun utf8ReaderBytesDirectNullable() = sumBytesReader(utf8DirectNullable)

    @Benchmark
    fun utf8ReaderBytesSelection() = sumBytesReader(utf8Selection)

    @Benchmark
    fun utf8ReaderBytesSelectionNullable() = sumBytesReader(utf8SelectionNullable)
}
