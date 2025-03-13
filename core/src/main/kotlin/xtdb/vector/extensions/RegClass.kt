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
import xtdb.types.RegClass
import xtdb.vector.ExtensionVectorWriter
import xtdb.vector.ValueVectorReader

class RegClassVector(name: String, allocator: BufferAllocator, fieldType: FieldType) :
    XtExtensionVector<IntVector>(name, allocator, fieldType, IntVector(name, allocator)) {

    init {
        require(fieldType.type == RegClassType)
    }

    override fun getObject0(index: Int): RegClass = RegClass(underlyingVector.get(index))
}

object RegClassType: XtExtensionType("xt/regclass", Types.MinorType.INT.getType()) {
    init {
        ExtensionTypeRegistry.register(this)
    }

    override fun deserialize(serializedData: String): ArrowType = this

    override fun getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator): FieldVector =
        RegClassVector(name, allocator, fieldType)
}

internal class RegClassReader(vec: RegClassVector): ValueVectorReader(vec) {
    private val underlyingVec = from(vec.underlyingVector)

    override fun getInt(idx: Int) = underlyingVec.getInt(idx)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = RegClass(getInt(idx))
}

internal class RegClassWriter(vector: RegClassVector) : ExtensionVectorWriter(vector, null) {
    override fun writeObject0(obj: Any) =
        if (obj !is RegClass) throw InvalidWriteObjectException(field.fieldType, obj)
        else super.writeObject0(obj.oid)

    override fun writeValue0(v: ValueReader) = writeBytes(v.readBytes())
}