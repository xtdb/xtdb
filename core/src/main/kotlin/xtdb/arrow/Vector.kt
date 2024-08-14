@file:JvmName("Vectors")

package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import org.apache.arrow.memory.util.hash.ArrowBufHasher
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
import org.apache.arrow.vector.types.pojo.Field
import xtdb.api.query.IKeyFn
import xtdb.vector.extensions.KeywordType
import xtdb.vector.extensions.TransitType
import xtdb.vector.extensions.UuidType
import java.time.ZoneId
import org.apache.arrow.vector.NullVector as ArrowNullVector

internal fun Any.unsupported(op: String): Nothing =
    throw UnsupportedOperationException("$op unsupported on ${this::class.simpleName}")

sealed class Vector : VectorReader, VectorWriter {

    abstract override var nullable: Boolean; internal set
    override var valueCount: Int = 0; internal set

    internal abstract fun getObject0(idx: Int, keyFn: IKeyFn<*>): Any
    override fun getObject(idx: Int, keyFn: IKeyFn<*>) = if (isNull(idx)) null else getObject0(idx, keyFn)

    protected abstract fun writeObject0(value: Any)
    override fun writeObject(value: Any?) = if (value == null) writeNull() else writeObject0(value)

    abstract fun hashCode0(idx: Int, hasher: ArrowBufHasher): Int
    final override fun hashCode(idx: Int, hasher: ArrowBufHasher) =
        if (isNull(idx)) ArrowBufPointer.NULL_HASH_CODE else hashCode0(idx, hasher)

    internal abstract fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)
    internal abstract fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)
    internal abstract fun loadFromArrow(vec: ValueVector)

    override fun toList() = (0 until valueCount).map { getObject(it) }

    override fun toString() = VectorReader.toString(this)

    companion object {
        @JvmStatic
        fun fromField(al: BufferAllocator, field: Field): Vector {
            val name: String = field.name
            val isNullable = field.fieldType.isNullable

            return field.type.accept(object : ArrowTypeVisitor<Vector> {
                override fun visit(type: Null) = NullVector(name)

                override fun visit(type: Struct) =
                    StructVector(
                        al, name, isNullable,
                        field.children.associateTo(linkedMapOf()) { it.name to fromField(al, it) }
                    )

                override fun visit(type: ArrowType.List) =
                    ListVector(al, name, isNullable, fromField(al, field.children[0]))

                override fun visit(type: LargeList) = TODO("Not yet implemented")

                override fun visit(type: FixedSizeList) =
                    FixedSizeListVector(al, name, isNullable, type.listSize, fromField(al, field.children[0]))

                override fun visit(type: ListView) = TODO("Not yet implemented")

                override fun visit(type: Union) = when (type.mode!!) {
                    UnionMode.Sparse -> TODO("Not yet implemented")
                    UnionMode.Dense -> DenseUnionVector(al, name, isNullable, field.children.map { fromField(al, it) })
                }

                override fun visit(type: ArrowType.Map): MapVector {
                    val structVec = fromField(al, field.children.first())
                    return MapVector(ListVector(al, name, isNullable, structVec), type.keysSorted)
                }

                override fun visit(type: Bool) = BitVector(al, name, isNullable)

                override fun visit(type: ArrowType.Int) = when (type.bitWidth) {
                    8 -> ByteVector(al, name, isNullable)
                    16 -> ShortVector(al, name, isNullable)
                    32 -> IntVector(al, name, isNullable)
                    64 -> LongVector(al, name, isNullable)
                    else -> error("invalid bit-width: ${type.bitWidth}")
                }

                override fun visit(type: FloatingPoint) = when (type.precision!!) {
                    HALF -> TODO("half precision not supported")
                    SINGLE -> FloatVector(al, name, isNullable)
                    DOUBLE -> DoubleVector(al, name, isNullable)
                }

                override fun visit(type: Decimal) = TODO("Not yet implemented")

                override fun visit(type: Utf8) = Utf8Vector(al, name, isNullable)
                override fun visit(type: Utf8View) = TODO("Not yet implemented")
                override fun visit(type: LargeUtf8) = TODO("Not yet implemented")

                override fun visit(type: Binary) = VarBinaryVector(al, name, isNullable)
                override fun visit(type: BinaryView) = TODO("Not yet implemented")
                override fun visit(type: LargeBinary) = TODO("Not yet implemented")
                override fun visit(type: FixedSizeBinary) = FixedSizeBinaryVector(al, name, isNullable, type.byteWidth)

                override fun visit(type: Date) = when (type.unit!!) {
                    DAY -> DateDayVector(al, name, isNullable)
                    DateUnit.MILLISECOND -> DateMilliVector(al, name, isNullable)
                }

                override fun visit(type: Time) = when (type.unit!!) {
                    SECOND, MILLISECOND -> Time32Vector(al, name, isNullable, type.unit)
                    MICROSECOND, NANOSECOND -> Time64Vector(al, name, isNullable, type.unit)
                }

                override fun visit(type: Timestamp) =
                    if (type.timezone == null) TimestampLocalVector(al, name, isNullable, type.unit)
                    else TimestampTzVector(al, name, isNullable, type.unit, ZoneId.of(type.timezone))

                override fun visit(type: Interval) = when (type.unit!!) {
                    YEAR_MONTH -> IntervalYearMonthVector(al, name, isNullable)
                    DAY_TIME -> IntervalDayTimeVector(al, name, isNullable)
                    MONTH_DAY_NANO -> IntervalMonthDayNanoVector(al, name, isNullable)
                }

                override fun visit(type: Duration) = DurationVector(al, name, isNullable, type.unit)

                override fun visit(type: ExtensionType) = when (type) {
                    KeywordType -> KeywordVector(Utf8Vector(al, name, isNullable))
                    UuidType -> UuidVector(FixedSizeBinaryVector(al, name, isNullable, 16))
                    TransitType -> TransitVector(VarBinaryVector(al, name, isNullable))
                    else -> error("unknown extension: $type")
                }
            })
        }

        @JvmStatic
        fun fromArrow(vec: ValueVector): Vector =
            (if (vec is ArrowNullVector) NullVector(vec.name) else fromField(vec.allocator, vec.field))
                .apply { loadFromArrow(vec) }
    }
}
