package xtdb.arrow

import xtdb.vector.IVectorWriter
import org.apache.arrow.vector.types.pojo.Field

interface ListExpression {
    val size: Int
    fun writeTo(vecWriter: IVectorWriter, start: Int, len: Int)
}
