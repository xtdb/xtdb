package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.FloatingPointPrecision.*
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

sealed class Vector : AutoCloseable {
    abstract val name: String
    abstract var nullable: Boolean

    var valueCount: Int = 0; protected set

    abstract val arrowField: Field

    private fun unsupported(op: String): Nothing =
        throw UnsupportedOperationException("$op unsupported on ${this::class.simpleName}")

    abstract fun isNull(idx: Int): Boolean
    abstract fun writeNull()

    open fun getByte(idx: Int): Byte = unsupported("getByte")
    open fun writeByte(value: Byte): Unit = unsupported("writeByte")

    open fun getShort(idx: Int): Short = unsupported("getShort")
    open fun writeShort(value: Short): Unit = unsupported("writeShort")

    open fun getInt(idx: Int): Int = unsupported("getInt")
    open fun writeInt(value: Int): Unit = unsupported("writeInt")

    open fun getLong(idx: Int): Long = unsupported("getLong")
    open fun writeLong(value: Long): Unit = unsupported("writeLong")

    open fun getFloat(idx: Int): Float = unsupported("getFloat")
    open fun writeFloat(value: Float): Unit = unsupported("writeFloat")

    open fun getDouble(idx: Int): Double = unsupported("getDouble")
    open fun writeDouble(value: Double): Unit = unsupported("writeDouble")

    open fun getBytes(idx: Int): ByteArray = unsupported("getBytes")
    open fun writeBytes(bytes: ByteArray): Unit = unsupported("writeBytes")

    protected abstract fun getObject0(idx: Int): Any

    fun getObject(idx: Int) = if (isNull(idx)) null else getObject0(idx)

    protected abstract fun writeObject0(value: Any)

    fun writeObject(value: Any?) {
        if (value == null) writeNull() else writeObject0(value)
    }

    internal abstract fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)
    internal abstract fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)

    abstract fun reset()

    companion object {
        fun fromField(field: Field, al: BufferAllocator): Vector {
            val name: String = field.name
            val isNullable = field.fieldType.isNullable

            return field.type.accept(object : ArrowType.ArrowTypeVisitor<Vector> {
                override fun visit(type: ArrowType.Null) = NullVector(name)

                override fun visit(type: ArrowType.Struct): Vector = TODO("Not yet implemented")

                override fun visit(type: ArrowType.List): Vector =
                    ListVector(al, name, isNullable, fromField(field.children[0], al))

                override fun visit(type: ArrowType.LargeList): Vector = TODO("Not yet implemented")
                override fun visit(type: ArrowType.FixedSizeList): Vector = TODO("Not yet implemented")
                override fun visit(type: ArrowType.ListView): Vector = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Union) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Map) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Bool) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Int) = when (type.bitWidth) {
                    8 -> ByteVector(al, name, isNullable)
                    16 -> ShortVector(al, name, isNullable)
                    32 -> IntVector(al, name, isNullable)
                    64 -> LongVector(al, name, isNullable)
                    else -> error("invalid bit-width: ${type.bitWidth}")
                }

                override fun visit(type: ArrowType.FloatingPoint) = when (type.precision!!) {
                    HALF -> TODO("half precision not supported")
                    SINGLE -> FloatVector(al, name, isNullable)
                    DOUBLE -> DoubleVector(al, name, isNullable)
                }

                override fun visit(type: ArrowType.Decimal) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Utf8) = Utf8Vector(al, name, isNullable)
                override fun visit(type: ArrowType.Utf8View) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.LargeUtf8) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Binary) = VarBinaryVector(al, name, isNullable)
                override fun visit(type: ArrowType.BinaryView) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.LargeBinary) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.FixedSizeBinary) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Date) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Time) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Timestamp) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Interval) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Duration) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.ExtensionType) = TODO("Not yet implemented")
            })
        }
    }
}
