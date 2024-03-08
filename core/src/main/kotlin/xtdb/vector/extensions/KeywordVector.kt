package xtdb.vector.extensions

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

class KeywordVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<VarCharVector>(name, allocator, fieldType, VarCharVector(name, allocator)) {

    override fun getObject(index: Int): Keyword? =
        if (isNull(index)) null else Keyword.intern(underlyingVector.getObject(index).toString())
}
