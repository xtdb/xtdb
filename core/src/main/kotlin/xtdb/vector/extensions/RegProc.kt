package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.arrow.InvalidWriteObjectException
import xtdb.arrow.ValueReader
import xtdb.types.RegProc
import xtdb.vector.ExtensionVectorWriter
import xtdb.vector.ValueVectorReader

class RegProcVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<IntVector>(name, allocator, fieldType, IntVector(name, allocator)) {

    init {
        require(fieldType.type == RegProcType)
    }

    override fun getObject0(index: Int): RegProc = RegProc(underlyingVector.get(index))
}

object RegProcType: XtExtensionType("xt/regproc", Types.MinorType.INT.getType()) {
    init {
        ExtensionTypeRegistry.register(this)
    }

    override fun deserialize(serializedData: String): ArrowType = this

    override fun getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator): FieldVector =
        RegProcVector(name, allocator, fieldType)
}

internal class RegProcReader(vec: RegProcVector): ValueVectorReader(vec) {
    private val underlyingVec = from(vec.underlyingVector)

    override fun getInt(idx: Int) = underlyingVec.getInt(idx)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = RegProc(getInt(idx))
}

internal class RegProcWriter(vector: RegProcVector) : ExtensionVectorWriter(vector, null) {
    override fun writeObject0(obj: Any) =
        if (obj !is RegProc) throw InvalidWriteObjectException(field.fieldType, obj)
        else super.writeObject0(obj.oid)

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}
