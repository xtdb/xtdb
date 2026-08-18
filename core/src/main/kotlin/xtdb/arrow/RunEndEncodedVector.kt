package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.util.Hasher
import xtdb.util.safelyOpening
import java.nio.ByteBuffer
import org.apache.arrow.vector.complex.RunEndEncodedVector as ArrowRunEndEncodedVector

/**
 * A column stored as runs of equal values: a `run_ends` child holding each run's exclusive logical end,
 * and a `values` child holding one value per run.
 *
 * Every accessor takes a **logical** index, so this reads exactly like the plain column it encodes, with
 * [runIdx] the only place the encoding surfaces.
 *
 * XTDB reads this encoding but does not yet write it, so that a node on this version can read a data file
 * a later version writes (#5914).
 */
class RunEndEncodedVector private constructor(
    override var name: String,
    private val runEndsVector: Vector,
    private val valuesVector: Vector,
    override var valueCount: Int
) : Vector(), LongLongVectorReader {

    init {
        require(runEndsVector.arrowType.let { it == I16_TYPE || it == I32_TYPE || it == I64_TYPE }) {
            "run-ends must be a signed integer vector, got ${runEndsVector.arrowType}"
        }

        require(!runEndsVector.nullable) { "run-ends must be non-nullable" }
    }

    constructor(name: String, runEndsVector: Vector, valuesVector: Vector)
            : this(name, runEndsVector, valuesVector, 0)

    constructor(al: BufferAllocator, name: String, valuesVector: Vector)
            : this(name, IntVector.open(al, REE_RUN_ENDS_NAME, false), valuesVector)

    override val arrowType get() = RUN_END_ENCODED_TYPE

    override val vectors get() = listOf(runEndsVector, valuesVector)

    /** The values' type: run-end encoding is a layout, not a type, so it doesn't show up here. */
    override val type get() = valuesVector.type

    // Having no validity buffer of its own, the parent is never nullable whatever its values are - while
    // whether the *column* admits nulls is whether those values do.
    override var nullable: Boolean
        get() = valuesVector.nullable
        set(value) {
            valuesVector.nullable = value
        }

    override val fieldType get() = FieldType(false, RUN_END_ENCODED_TYPE, null)

    val runEnds: VectorReader get() = runEndsVector

    /** One value per run, indexed by run rather than by logical row. */
    val runValues: VectorReader get() = valuesVector

    val runCount get() = runEndsVector.valueCount

    fun runEnd(runIdx: Int) = runEndsVector.getLong(runIdx).toInt()

    fun runStart(runIdx: Int) = if (runIdx == 0) 0 else runEnd(runIdx - 1)

    // A stale hint is harmless - [runIdx] checks one covers the index before trusting it - so this needs
    // no synchronising, given int writes don't tear.
    private var runIdxHint = 0

    fun runIdx(idx: Int): Int {
        val hint = runIdxHint

        return when {
            hint < runCount && idx >= runStart(hint) && idx < runEnd(hint) -> hint
            hint + 1 < runCount && idx >= runStart(hint + 1) && idx < runEnd(hint + 1) -> hint + 1
            else -> searchRunIdx(idx)
        }.also { runIdxHint = it }
    }

    private fun searchRunIdx(idx: Int): Int {
        var lo = 0
        var hi = runCount

        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (runEnd(mid) <= idx) lo = mid + 1 else hi = mid
        }

        return lo
    }

    override fun isNull(idx: Int) = valuesVector.isNull(runIdx(idx))

    override fun getBoolean(idx: Int) = valuesVector.getBoolean(runIdx(idx))
    override fun getByte(idx: Int) = valuesVector.getByte(runIdx(idx))
    override fun getShort(idx: Int) = valuesVector.getShort(runIdx(idx))
    override fun getInt(idx: Int) = valuesVector.getInt(runIdx(idx))
    override fun getLong(idx: Int) = valuesVector.getLong(runIdx(idx))
    override fun getFloat(idx: Int) = valuesVector.getFloat(runIdx(idx))
    override fun getDouble(idx: Int) = valuesVector.getDouble(runIdx(idx))
    override fun getBytes(idx: Int): ByteBuffer = valuesVector.getBytes(runIdx(idx))
    override fun getPointer(idx: Int, reuse: ArrowBufPointer) = valuesVector.getPointer(runIdx(idx), reuse)
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = valuesVector.getObject(runIdx(idx), keyFn)

    override fun getLongLongHigh(idx: Int) = (valuesVector as LongLongVectorReader).getLongLongHigh(runIdx(idx))
    override fun getLongLongLow(idx: Int) = (valuesVector as LongLongVectorReader).getLongLongLow(runIdx(idx))

    override fun hashCode(idx: Int, hasher: Hasher) = valuesVector.hashCode(runIdx(idx), hasher)

    override fun getLeg(idx: Int) = valuesVector.getLeg(runIdx(idx))

    // A child here is indexed by run, not by row, so nothing is handed out or advertised - composite
    // values still read whole, through [getObject].
    override val keyNames: Set<String>? get() = null
    override val legNames: Set<String>? get() = null

    override fun vectorForOrNull(name: String): VectorWriter =
        throw UnsupportedOperationException(
            "run-encoded columns don't expose their children - their values are indexed by run, not by row"
        )

    /** Runs cover the same distinct values as the rows they encode, so metadata is the same either way. */
    override val metadataFlavours get() = valuesVector.metadataFlavours

    override fun valueReader(): ValueReader {
        val inner = valuesVector.valueReader()

        return object : ValueReader by inner {
            override var pos = 0
                set(value) {
                    field = value
                    if (value in 0..<valueCount) inner.pos = runIdx(value)
                }
        }
    }

    override fun rowCopier(dest: VectorWriter): RowCopier =
        if (dest is RunEndEncodedVector) RunCopier(valuesVector.rowCopier(dest.valuesVector), dest)
        else valuesVector.rowCopier(dest).let { copier -> RowCopier { srcIdx -> copier.copyRow(runIdx(srcIdx)) } }

    private inner class RunCopier(
        private val valueCopier: RowCopier, private val dest: RunEndEncodedVector
    ) : RowCopier {

        override fun copyRow(srcIdx: Int) {
            valueCopier.copyRow(runIdx(srcIdx))
            dest.endRun(1)
        }

        override fun copyRange(startIdx: Int, len: Int) {
            var idx = startIdx
            val endIdx = startIdx + len

            while (idx < endIdx) {
                val run = runIdx(idx)
                val copyTo = minOf(runEnd(run), endIdx)

                valueCopier.copyRow(run)
                dest.endRun(copyTo - idx)

                idx = copyTo
            }
        }
    }

    /** Equal neighbours are not coalesced. */
    @JvmOverloads
    fun writeRun(value: Any?, runLength: Int = 1) {
        valuesVector.writeObject(value)
        endRun(runLength)
    }

    private inline fun writeSingletonRun(writeValue: (VectorWriter) -> Unit) {
        writeValue(valuesVector)
        endRun(1)
    }

    override fun writeObject(obj: Any?) = writeRun(obj)
    override fun writeValue0(v: ValueReader) = writeSingletonRun { it.writeValue(v) }
    override fun writeUndefined() = writeSingletonRun { it.writeUndefined() }

    override fun writeBoolean(v: Boolean) = writeSingletonRun { it.writeBoolean(v) }
    override fun writeByte(v: Byte) = writeSingletonRun { it.writeByte(v) }
    override fun writeShort(v: Short) = writeSingletonRun { it.writeShort(v) }
    override fun writeInt(v: Int) = writeSingletonRun { it.writeInt(v) }
    override fun writeLong(v: Long) = writeSingletonRun { it.writeLong(v) }
    override fun writeFloat(v: Float) = writeSingletonRun { it.writeFloat(v) }
    override fun writeDouble(v: Double) = writeSingletonRun { it.writeDouble(v) }
    override fun writeBytes(v: ByteBuffer) = writeSingletonRun { it.writeBytes(v) }

    private fun endRun(runLength: Int) {
        require(runLength > 0) { "run length must be positive, got $runLength" }

        check(valuesVector.valueCount == runCount + 1) {
            "expected exactly one value written for the run, got ${valuesVector.valueCount - runCount}"
        }

        val runEnd = valueCount + runLength

        when (runEndsVector) {
            is ShortVector -> {
                require(runEnd <= Short.MAX_VALUE) { "$runEnd rows overflow int16 run-ends" }
                runEndsVector.writeShort(runEnd.toShort())
            }

            is IntVector -> runEndsVector.writeInt(runEnd)
            is LongVector -> runEndsVector.writeLong(runEnd.toLong())
            else -> error("unreachable: run-ends type checked at construction")
        }

        valueCount = runEnd
    }

    // Unreachable: an unencoded source is turned away by the arrow-type check in [Vector.rowCopier] before
    // it gets here, and an encoded one is served by the override above.
    override fun rowCopier0(src: VectorReader) = unsupported("rowCopier0")

    /** An encoded column's type is pinned by the values it already holds. */
    override fun maybePromote(al: BufferAllocator, targetType: ArrowType, targetNullable: Boolean) =
        unsupported("maybePromote")

    // The run-ends are absolute from the start of the vector, and a slice here carries the whole vector
    // rather than a logical offset into it, so they stay valid as they are.
    override fun openSlice(al: BufferAllocator) = safelyOpening {
        val runEndsSlice = open { runEndsVector.openSlice(al) }
        val valuesSlice = open { valuesVector.openSlice(al) }

        RunEndEncodedVector(name, runEndsSlice, valuesSlice, valueCount)
    }

    // An REE parent carries no buffers of its own - the encoding lives entirely in its two children.
    override fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        nodes.add(ArrowFieldNode(valueCount.toLong(), 0))

        runEndsVector.unloadPage(nodes, buffers)
        valuesVector.unloadPage(nodes, buffers)
    }

    override fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>) {
        val node = nodes.removeFirstOrNull() ?: error("missing node")
        valueCount = node.length
        runIdxHint = 0

        runEndsVector.loadPage(nodes, buffers)
        valuesVector.loadPage(nodes, buffers)

        checkRuns()
    }

    override fun loadFromArrow(vec: ValueVector) {
        require(vec is ArrowRunEndEncodedVector)

        runEndsVector.loadFromArrow(vec.runEndsVector)
        valuesVector.loadFromArrow(vec.valuesVector)

        valueCount = vec.valueCount
        runIdxHint = 0

        checkRuns()
    }

    private fun checkRuns() {
        check(runCount == valuesVector.valueCount) {
            "$runCount runs against ${valuesVector.valueCount} values"
        }

        var prev = 0L

        for (i in 0..<runCount) {
            val end = runEndsVector.getLong(i)
            check(end in prev + 1..Int.MAX_VALUE.toLong()) { "run end $end at run $i doesn't follow $prev" }
            prev = end
        }

        // `>=`, because the spec allows a logical length shorter than the runs covering it.
        check(prev >= valueCount) { "run ends cover $prev of $valueCount rows" }
    }

    override fun clear() {
        runEndsVector.clear()
        valuesVector.clear()
        valueCount = 0
        runIdxHint = 0
    }

    override fun close() {
        runEndsVector.close()
        valuesVector.close()
        valueCount = 0
        runIdxHint = 0
    }
}
