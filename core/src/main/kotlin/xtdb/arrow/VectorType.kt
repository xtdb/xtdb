package xtdb.arrow

sealed interface VectorType

sealed class IntType(bits: Byte) : VectorType

data object Int32Type : IntType(32)

