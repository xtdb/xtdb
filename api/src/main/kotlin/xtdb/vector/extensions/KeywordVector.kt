package xtdb.vector.extensions

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.types.pojo.FieldType

class KeywordVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<VarCharVector>(name, allocator, fieldType, VarCharVector(name, allocator)) {

    init {
        require(fieldType.type == KeywordType)
    }

    override fun getObject0(index: Int): Keyword = Keyword.intern(underlyingVector.getObject(index).toString())
}
