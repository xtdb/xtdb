@file:JvmName("Vectors")

package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
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
import xtdb.api.query.IKeyFn
import xtdb.trie.ColumnName
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.util.Hasher
import xtdb.util.closeAllOnCatch
import xtdb.util.safeMap
import xtdb.vector.extensions.*
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

    abstract override var name: String
    abstract override var nullable: Boolean
    abstract override val arrowType: ArrowType
    abstract val vectors: Iterable<Vector>

    override val childFields get() = vectors.map { it.field }
    final override val field: Field get() = Field(name, FieldType(nullable, arrowType, null), vectors.map { it.field })

    abstract override var valueCount: Int; internal set

    internal abstract fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = if (isNull(idx)) null else getObject0(idx, keyFn)

    open fun ensureCapacity(valueCount: Int): Unit = unsupported("ensureCapacity")

    open fun setNull(idx: Int): Unit = unsupported("setNull")
    open fun setBoolean(idx: Int, v: Boolean): Unit = unsupported("setBoolean")
    open fun setInt(idx: Int, v: Int): Unit = unsupported("setInt")
    open fun setLong(idx: Int, v: Long): Unit = unsupported("setLong")
    open fun setFloat(idx: Int, v: Float): Unit = unsupported("setFloat")
    open fun setDouble(idx: Int, v: Double): Unit = unsupported("setDouble")

    override fun writeNull() {
        if (!nullable) nullable = true
        writeUndefined()
    }

    protected abstract fun writeObject0(value: Any)

    override fun writeObject(obj: Any?) =
        when (obj) {
            null -> writeNull()
            is ValueReader -> writeValue(obj)
            else -> writeObject0(obj)
        }

    abstract fun hashCode0(idx: Int, hasher: Hasher): Int
    final override fun hashCode(idx: Int, hasher: Hasher) =
        if (isNull(idx)) ArrowBufPointer.NULL_HASH_CODE else hashCode0(idx, hasher)

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
    internal abstract fun loadPage(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)
    internal abstract fun loadFromArrow(vec: ValueVector)

    internal open fun maybePromote(al: BufferAllocator, targetType: ArrowType, targetNullable: Boolean): Vector =
        // if it's a NullVector coming in, don't promote - we can just set ourselves to nullable. #4675
        when {
            targetType != arrowType && targetType != NULL_TYPE ->
                DenseUnionVector(al, name, listOf(this), valueCount)
                    .apply {
                        repeat(this.valueCount) { idx ->
                            typeBuffer.writeByte(0)
                            offsetBuffer.writeInt(idx)
                        }

                        maybePromote(al, targetType, targetNullable)
                    }
                    .also {
                        this@Vector.name = arrowType.toLeg()
                    }

            else -> {
                nullable = nullable || targetNullable
                this
            }
        }

    override fun rowCopier(dest: VectorWriter): RowCopier {
        if (dest is DenseUnionVector.LegVector) return dest.rowCopierFrom(this)
        if (dest is DenseUnionVector) return dest.rowCopier0(this)

        check(dest is Vector) { "can only copy to another Vector, got ${dest::class}" }
        
        if (arrowType != dest.arrowType) throw InvalidCopySourceException(this, dest)

        return dest.rowCopier0(this)
    }

    internal abstract fun rowCopier0(src: VectorReader): RowCopier

    override val asList get() = (0 until valueCount).map { getObject(it) }

    override fun toString() = VectorReader.toString(this)

    companion object {
        @JvmStatic
        @JvmName("open")
        fun BufferAllocator.openVector(field: Field): Vector =
            field.children.safeMap { openVector(it) }.closeAllOnCatch { children ->
                openVector(field.name, field.type, field.isNullable, children)
            }

        @JvmStatic
        @JvmName("open")
        fun BufferAllocator.openVector(name: FieldName, type: VectorType): Vector =
            type.children.entries.safeMap { (n, t) -> openVector(n, t) }.closeAllOnCatch { children ->
                openVector(name, type.arrowType, type.nullable, children)
            }

        @JvmStatic
        @JvmName("open")
        fun BufferAllocator.openVector(
            name: FieldName,
            arrowType: ArrowType,
            nullable: Boolean,
            children: List<Vector> = emptyList()
        ): Vector {
            val al = this

            return arrowType.accept(object : ArrowTypeVisitor<Vector> {
                override fun visit(type: Null) = NullVector(name)

                override fun visit(type: Struct) =
                    StructVector(al, name, nullable, children.associateTo(linkedMapOf()) { it.name to it })

                override fun visit(type: ArrowType.List) =
                    ListVector(al, name, nullable, children.firstOrNull() ?: NullVector($$"$data$", false))

                override fun visit(type: LargeList) = TODO("Not yet implemented")

                override fun visit(type: FixedSizeList) =
                    FixedSizeListVector(al, name, nullable, type.listSize, children.firstOrNull() ?: NullVector($$"$data$", false))

                override fun visit(type: ListView) = TODO("Not yet implemented")
                override fun visit(type: LargeListView) = TODO("Not yet implemented")

                override fun visit(type: Union) = when (type.mode!!) {
                    UnionMode.Sparse -> TODO("Not yet implemented")
                    UnionMode.Dense -> DenseUnionVector(al, name, children, 0)
                }

                override fun visit(type: ArrowType.Map): MapVector {
                    val structVec = children.first()
                    return MapVector(ListVector(al, name, nullable, structVec), type.keysSorted)
                }

                override fun visit(type: Bool) = BitVector(al, name, nullable)

                override fun visit(type: ArrowType.Int): Vector = when (type.bitWidth) {
                    8 -> ByteVector(al, name, nullable)
                    16 -> ShortVector(al, name, nullable)
                    32 -> IntVector.open(al, name, nullable)
                    64 -> LongVector(al, name, nullable)
                    else -> error("invalid bit-width: ${type.bitWidth}")
                }

                override fun visit(type: FloatingPoint): Vector = when (type.precision!!) {
                    HALF -> error("half precision not supported")
                    SINGLE -> FloatVector(al, name, nullable)
                    DOUBLE -> DoubleVector(al, name, nullable)
                }

                override fun visit(type: Decimal) = DecimalVector(al, name, nullable, type)

                override fun visit(type: Utf8) = Utf8Vector(al, name, nullable)
                override fun visit(type: Utf8View) = TODO("Not yet implemented")
                override fun visit(type: LargeUtf8) = TODO("Not yet implemented")

                override fun visit(type: Binary) = VarBinaryVector(al, name, nullable)
                override fun visit(type: BinaryView) = TODO("Not yet implemented")
                override fun visit(type: LargeBinary) = TODO("Not yet implemented")
                override fun visit(type: FixedSizeBinary) = FixedSizeBinaryVector(al, name, nullable, type.byteWidth)

                override fun visit(type: Date): Vector = when (type.unit!!) {
                    DAY -> DateDayVector(al, name, nullable)
                    DateUnit.MILLISECOND -> DateMilliVector(al, name, nullable)
                }

                override fun visit(type: Time): Vector = when (type.unit!!) {
                    SECOND, MILLISECOND -> Time32Vector(al, name, nullable, type.unit)
                    MICROSECOND, NANOSECOND -> Time64Vector(al, name, nullable, type.unit)
                }

                override fun visit(type: Timestamp): Vector =
                    if (type.timezone == null) TimestampLocalVector(al, name, nullable, type.unit)
                    else TimestampTzVector(al, name, nullable, type.unit, ZoneId.of(type.timezone))

                override fun visit(type: Interval): Vector = when (type.unit!!) {
                    YEAR_MONTH -> IntervalYearMonthVector(al, name, nullable)
                    DAY_TIME -> IntervalDayTimeVector(al, name, nullable)
                    MONTH_DAY_NANO -> IntervalMonthDayNanoVector(al, name, nullable)
                }

                override fun visit(type: Duration) = DurationVector(al, name, nullable, type.unit)

                override fun visit(p0: RunEndEncoded?) = TODO("Not yet implemented")

                override fun visit(type: ExtensionType): Vector = when (type) {
                    KeywordType -> KeywordVector(Utf8Vector(al, name, nullable))
                    UuidType -> UuidVector(FixedSizeBinaryVector(al, name, nullable, 16))
                    UriType -> UriVector(Utf8Vector(al, name, nullable))
                    TransitType -> TransitVector(VarBinaryVector(al, name, nullable))
                    IntervalMDMType -> IntervalMonthDayMicroVector(IntervalMonthDayNanoVector(al, name, nullable))
                    TsTzRangeType -> TsTzRangeVector(
                        FixedSizeListVector(al, name, nullable, 2, children.firstOrNull() ?: NullVector($$"$data$", false))
                    )

                    SetType ->
                        SetVector(
                            ListVector(al, name, nullable, children.firstOrNull() ?: NullVector($$"$data$", false))
                        )

                    OidType -> OidVector(IntVector.open(al, name, nullable))
                    RegClassType -> RegClassVector(IntVector.open(al, name, nullable))
                    RegProcType -> RegProcVector(IntVector.open(al, name, nullable))

                    else -> error("unknown extension: $type")
                }
            })
        }

        @JvmStatic
        fun fromArrow(vec: ValueVector): Vector {
            val vector =
                if (vec is ArrowNullVector) NullVector(vec.name, vec.field.isNullable)
                else vec.allocator.openVector(vec.field)

            return vector.apply { loadFromArrow(vec) }
        }

        @JvmStatic
        fun fromList(al: BufferAllocator, field: Field, values: List<*>): Vector {
            var vec = al.openVector(field)
            try {
                for (value in values) {
                    try {
                        vec.writeObject(value)
                    } catch (_: InvalidWriteObjectException) {
                        vec = vec.maybePromote(al, value.toArrowType(), value == null)
                        vec.writeObject(value)
                    }
                }
                return vec
            } catch (t: Throwable) {
                vec.close()
                throw t
            }
        }

        @JvmStatic
        fun fromList(al: BufferAllocator, colName: FieldName, type: VectorType, values: List<*>) =
            fromList(al, colName ofType type, values)

        @JvmStatic
        fun fromList(al: BufferAllocator, name: ColumnName, values: List<*>) =
            fromList(al, name ofType VectorType.NULL, values)
    }
}
