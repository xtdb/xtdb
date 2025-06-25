package xtdb.arrow

interface ListExpression {
    val size: Int
    fun writeTo(vecWriter: VectorWriter, start: Int, len: Int)
}
