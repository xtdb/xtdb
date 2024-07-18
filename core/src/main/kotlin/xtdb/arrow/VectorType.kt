package xtdb.arrow

import org.apache.arrow.vector.types.pojo.ArrowType

sealed class VectorType {
    internal abstract val arrowType: ArrowType
}

sealed class IntType(bits: Int) : VectorType() {
    override val arrowType = ArrowType.Int(bits, true)
}

data object Int32Type : IntType(32)

