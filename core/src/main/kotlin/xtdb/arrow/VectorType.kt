package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor

sealed class VectorType {
    internal abstract val arrowType: ArrowType

    abstract fun newVector(field: Field, al: BufferAllocator): Vector

    companion object {
        fun from(type: ArrowType): VectorType {
            return type.accept(object : ArrowTypeVisitor<VectorType> {
                override fun visit(type: ArrowType.Null?): VectorType {
                    TODO("Not yet implemented")
                }

                override fun visit(type: ArrowType.Struct) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.List) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.LargeList) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.FixedSizeList) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.ListView) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Union) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Map) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Bool) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Int) = when (type.bitWidth) {
                    32 -> Int32Type
                    else -> error("unsupported bit width: ${type.bitWidth}")
                }

                override fun visit(type: ArrowType.FloatingPoint) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Decimal) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Date) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Time) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Timestamp) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Interval) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Duration) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Utf8) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.Utf8View) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.LargeUtf8) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.Binary) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.BinaryView) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.LargeBinary) = TODO("Not yet implemented")
                override fun visit(type: ArrowType.FixedSizeBinary) = TODO("Not yet implemented")

                override fun visit(type: ArrowType.ExtensionType) = TODO("Not yet implemented")
            })
        }
    }
}

sealed class IntType(bits: Int) : VectorType() {
    override val arrowType = ArrowType.Int(bits, true)
}

data object Int32Type : IntType(32) {
    override fun newVector(field: Field, al: BufferAllocator) = IntVector(field, al)
}

