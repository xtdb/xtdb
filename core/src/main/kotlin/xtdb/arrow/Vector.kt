@file:JvmName("Vectors")

package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.DateUnit.DAY
import org.apache.arrow.vector.types.FloatingPointPrecision.*
import org.apache.arrow.vector.types.IntervalUnit.*
import org.apache.arrow.vector.types.TimeUnit.*
import org.apache.arrow.vector.types.UnionMode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.*
import org.apache.arrow.vector.types.pojo.DictionaryEncoding
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.InternalApi
import xtdb.arrow.VectorIndirection.Mask
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.extensions.IntervalMDMType
import xtdb.arrow.extensions.KeywordType
import xtdb.arrow.extensions.OidType
import xtdb.arrow.extensions.RegClassType
import xtdb.arrow.extensions.RegProcType
import xtdb.arrow.extensions.SetType
import xtdb.arrow.extensions.TransitType
import xtdb.arrow.extensions.TsTzRangeType
import xtdb.arrow.extensions.UriType
import xtdb.arrow.extensions.UuidType
import xtdb.trie.ColumnName
import xtdb.util.Hasher
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import xtdb.util.safeMap
import java.time.LocalDate
import java.time.ZoneId
import org.apache.arrow.vector.NullVector as ArrowNullVector

internal fun Any.unsupported(op: String): Nothing =
    throw UnsupportedOperationException("$op unsupported on ${this::class.simpleName}")

fun FieldType.copy(
    nullable: Boolean = isNullable,
    type: ArrowType = this.type,
    dictionary: DictionaryEncoding? = this.dictionary
) = FieldType(nullable, type, dictionary)

sealed class Vector : VectorReader, VectorWriter {

    abstract override var nullable: Boolean
    abstract override val arrowType: ArrowType
    abstract val vectors: Iterable<Vector>

    override val childFields: List<Field> get() = emptyList()

    abstract override var valueCount: Int; internal set

    abstract override fun hashTo(out: IntVector, hasher: Hasher, sel: IntArray?)

    /**
     * Compares each row of [mask] with [rhs]: the answer has the rows of [mask] that are non-null and compare true.
     * [sel] picks this vector's rows, and [mask] is over the selection's positions: bit `k` of [mask] is row `sel[k]`, or row `k` where [sel] is null.
     * A null answer means this vector doesn't cover the comparison with that scalar's domain.
     */
    open fun compare(op: CmpOp, rhs: Long, sel: IntArray?, mask: Mask): Mask? = null

    /** As the [Long] overload. */
    open fun compare(op: CmpOp, rhs: Double, sel: IntArray?, mask: Mask): Mask? = null

    /** As the [Long] overload. */
    open fun compare(op: CmpOp, rhs: LocalDate, sel: IntArray?, mask: Mask): Mask? = null

    /** The rows of [mask] that are non-null, with [sel] and [mask] as for [compare]; null where this vector doesn't cover it. */
    open fun notNull(sel: IntArray?, mask: Mask): Mask? = null

    override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    fun writeValues(al: BufferAllocator, values: Iterable<*>): Vector {
        var vec = this
        for (value in values) {
            try {
                vec.writeObject(value)
            } catch (_: InvalidWriteObjectException) {
                vec = vec.maybePromote(al, value.toArrowType(), value == null)
                vec.writeObject(value)
            }
        }
        return vec
    }

    override fun rowCopier(dest: VectorWriter): RowCopier {
        if (dest is DenseUnionVector.LegVector) return dest.rowCopierFrom(this)
        if (dest is DenseUnionVector) return dest.rowCopier0(this)

        check(dest is Vector) { "can only copy to another Vector, got ${dest::class}" }

        if (arrowType != dest.arrowType) throw InvalidCopySourceException(this, dest)

        return dest.rowCopier0(this)
    }

    internal abstract fun rowCopier0(src: VectorReader): RowCopier

    /**
     * Divides this vector by [divisorVec] and writes the result into [outVec].
     * Returns null for any element where the divisor is zero (to avoid NaN).
     */
    open fun divideInto(divisorVec: Vector, outVec: Vector): Vector = unsupported("divideInto")
    override fun squareInto(outVec: Vector): Vector = unsupported("squareInto")
    override fun sqrtInto(outVec: Vector): Vector = unsupported("sqrtInto")

    abstract override fun openSlice(al: BufferAllocator): Vector
    override fun openDirectSlice(al: BufferAllocator) = openSlice(al)

    internal abstract fun unloadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)

    /**
     * Unloads this page in the layout of [target] - see [ConformingUnloader].
     * Vectors with children override this to match them to the target's by name.
     */
    internal open fun unloadPage(target: Field, unloader: ConformingUnloader) =
        if (target.type == arrowType && vectors.none()) unloadPage(unloader.nodes, unloader.buffers)
        else unloader.copy(this, target)

    @InternalApi
    abstract override fun write(out: PageOutput, startIdx: Int, len: Int)

    internal abstract fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)
    internal abstract fun loadFromArrow(vec: ValueVector)

    internal abstract fun maybePromote(al: BufferAllocator, targetType: ArrowType, targetNullable: Boolean): Vector

    override fun toString() = VectorReader.toString(this)

    companion object {
        /** Opens an empty vector of [field]'s layout; [field]'s own name is the parent's to keep, so it's ignored here. */
        @JvmStatic
        @JvmName("open")
        fun BufferAllocator.openVector(field: Field): Vector = openVector(field.type, field.isNullable, field.children)

        @JvmStatic
        @JvmName("open")
        fun BufferAllocator.openVector(type: VectorType): Vector = openVector(type.toField(""))

        @JvmStatic
        @JvmName("open")
        fun BufferAllocator.openVector(
            arrowType: ArrowType,
            nullable: Boolean,
            children: List<Field> = emptyList()
        ): Vector =
            children.safeMap { openVector(it) }.closeAllOnCatch { childVecs ->
                openVector(arrowType, nullable, children.map { it.name }, childVecs)
            }

        private fun BufferAllocator.openVector(
            arrowType: ArrowType, nullable: Boolean, childNames: List<String>, childVecs: List<Vector>
        ): Vector {
            val al = this

            return arrowType.accept(object : ArrowTypeVisitor<Vector> {
                override fun visit(type: Null) = NullVector(nullable)

                override fun visit(type: Struct) =
                    StructVector(al, nullable, childNames.zip(childVecs).toMap(linkedMapOf()))

                override fun visit(type: ArrowType.List) =
                    ListVector(al, nullable, childVecs.firstOrNull() ?: NullVector(false))

                override fun visit(type: LargeList) = TODO("Not yet implemented")

                override fun visit(type: FixedSizeList) =
                    FixedSizeListVector(al, nullable, type.listSize, childVecs.firstOrNull() ?: NullVector(false))

                override fun visit(type: ListView) = TODO("Not yet implemented")
                override fun visit(type: LargeListView) = TODO("Not yet implemented")

                override fun visit(type: Union) = when (type.mode!!) {
                    UnionMode.Sparse -> TODO("Not yet implemented")
                    UnionMode.Dense -> DenseUnionVector(al, childNames, childVecs, 0)
                }

                override fun visit(type: ArrowType.Map): MapVector {
                    val structVec = childVecs.first()
                    return MapVector(ListVector(al, nullable, structVec), type.keysSorted)
                }

                override fun visit(type: Bool) = BitVector(al, nullable)

                override fun visit(type: ArrowType.Int): Vector = when (type.bitWidth) {
                    8 -> ByteVector(al, nullable)
                    16 -> ShortVector(al, nullable)
                    32 -> IntVector.open(al, nullable)
                    64 -> LongVector(al, nullable)
                    else -> error("invalid bit-width: ${type.bitWidth}")
                }

                override fun visit(type: FloatingPoint): Vector = when (type.precision!!) {
                    HALF -> error("half precision not supported")
                    SINGLE -> FloatVector(al, nullable)
                    DOUBLE -> DoubleVector(al, nullable)
                }

                override fun visit(type: Decimal) = DecimalVector(al, nullable, type)

                override fun visit(type: Utf8) = Utf8Vector(al, nullable)
                override fun visit(type: Utf8View) = TODO("Not yet implemented")
                override fun visit(type: LargeUtf8) = TODO("Not yet implemented")

                override fun visit(type: Binary) = VarBinaryVector(al, nullable)
                override fun visit(type: BinaryView) = TODO("Not yet implemented")
                override fun visit(type: LargeBinary) = TODO("Not yet implemented")
                override fun visit(type: FixedSizeBinary) = FixedSizeBinaryVector(al, nullable, type.byteWidth)

                override fun visit(type: Date): Vector = when (type.unit!!) {
                    DAY -> DateDayVector(al, nullable)
                    DateUnit.MILLISECOND -> DateMilliVector(al, nullable)
                }

                override fun visit(type: Time): Vector = when (type.unit!!) {
                    SECOND, MILLISECOND -> Time32Vector(al, nullable, type.unit)
                    MICROSECOND, NANOSECOND -> Time64Vector(al, nullable, type.unit)
                }

                override fun visit(type: Timestamp): Vector =
                    if (type.timezone == null) TimestampLocalVector(al, nullable, type.unit)
                    else TimestampTzVector(al, nullable, type.unit, ZoneId.of(type.timezone))

                override fun visit(type: Interval): Vector = when (type.unit!!) {
                    YEAR_MONTH -> IntervalYearMonthVector(al, nullable)
                    DAY_TIME -> IntervalDayTimeVector(al, nullable)
                    MONTH_DAY_NANO -> IntervalMonthDayNanoVector(al, nullable)
                }

                override fun visit(type: Duration) = DurationVector(al, nullable, type.unit)

                override fun visit(type: RunEndEncoded): Vector {
                    require(childVecs.size == 2) { "run-end encoded fields have exactly two children" }
                    val runEnds = childVecs[0]
                    require(runEnds is IntegerVector) { "run-ends must be a signed integer vector, got ${runEnds.arrowType}" }
                    return RunEndEncodedVector(runEnds, childVecs[1])
                }

                override fun visit(type: ExtensionType): Vector = when (type) {
                    KeywordType -> KeywordVector(Utf8Vector(al, nullable))
                    UuidType -> UuidVector(FixedSizeBinaryVector(al, nullable, 16))
                    UriType -> UriVector(Utf8Vector(al, nullable))
                    TransitType -> TransitVector(VarBinaryVector(al, nullable))
                    IntervalMDMType -> IntervalMonthDayMicroVector(IntervalMonthDayNanoVector(al, nullable))
                    TsTzRangeType -> TsTzRangeVector(
                        FixedSizeListVector(al, nullable, 2, childVecs.firstOrNull() ?: NullVector(false))
                    )

                    SetType ->
                        SetVector(
                            ListVector(al, nullable, childVecs.firstOrNull() ?: NullVector(false))
                        )

                    OidType -> OidVector(IntVector.open(al, nullable))
                    RegClassType -> RegClassVector(IntVector.open(al, nullable))
                    RegProcType -> RegProcVector(IntVector.open(al, nullable))

                    else -> error("unknown extension: $type")
                }
            })
        }

        @JvmStatic
        fun fromArrow(vec: ValueVector): Vector {
            val vector =
                if (vec is ArrowNullVector) NullVector(vec.field.isNullable)
                else vec.allocator.openVector(vec.field)

            return vector.closeOnCatch { it.apply { loadFromArrow(vec) } }
        }

        @JvmStatic
        fun fromList(al: BufferAllocator, type: VectorType, values: List<*>) =
            al.openVector(type).closeOnCatch { it.writeValues(al, values) }

        @JvmStatic
        fun fromList(al: BufferAllocator, values: List<*>) =
            fromList(al, VectorType.Null, values)
    }
}
