package xtdb.arrow

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

sealed class Vector : AutoCloseable {
    abstract val name: String
    abstract var nullable: Boolean
    abstract val valueCount: Int

    abstract val arrowField: Field

    private fun unsupported(op: String): Nothing =
        throw UnsupportedOperationException("$op unsupported on ${this::class.simpleName}")

    abstract fun isNull(idx: Int): Boolean
    abstract fun writeNull()

    open fun getInt(idx: Int): Int = unsupported("getInt")
    open fun setInt(idx: Int, value: Int): Unit = unsupported("setInt")
    open fun writeInt(value: Int): Unit = unsupported("writeInt")

    protected open fun getObject0(idx: Int): Any = unsupported("getObject")

    fun getObject(idx: Int) = if (isNull(idx)) null else getObject0(idx)

    abstract fun writeObject0(value: Any)

    fun writeObject(value: Any?) {
        if (value == null) writeNull() else writeObject0(value)
    }

    internal abstract fun unloadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)
    internal abstract fun loadBatch(nodes: MutableList<ArrowFieldNode>, buffers: MutableList<ArrowBuf>)

    abstract fun reset()

    companion object {
        fun fromField(field: Field, al: BufferAllocator): Vector =
            field.type.accept(object : ArrowType.ArrowTypeVisitor<Vector> {
                override fun visit(type: ArrowType.Null) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Struct): Vector = TODO("Not yet implemented")

                override fun visit(type: ArrowType.List): Vector = TODO("Not yet implemented")
                override fun visit(type: ArrowType.LargeList): Vector = TODO("Not yet implemented")
                override fun visit(type: ArrowType.FixedSizeList): Vector = TODO("Not yet implemented")
                override fun visit(type: ArrowType.ListView): Vector = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Union) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Map) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Bool) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Int) = when (type.bitWidth) {
                    32 -> IntVector(al, field.name, field.fieldType.isNullable)
                    else -> error("invalid bit-width: ${type.bitWidth}")
                }

                override fun visit(type: ArrowType.FloatingPoint) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Decimal) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Utf8) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Utf8View) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.LargeUtf8) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Binary) = TODO("Not yet implemented")
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
