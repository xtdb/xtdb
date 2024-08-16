package xtdb.arrow

import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.api.query.IKeyFn
import xtdb.vector.extensions.SetType

class SetVector(override val inner: ListVector) : ExtensionVector() {
    override val field
        get() = Field(name, FieldType(nullable, SetType, null), inner.field.children)

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = inner.getObject0(idx, keyFn).toSet()

    override fun writeObject0(value: Any) =
        if (value !is Set<*>) TODO("promotion ${value::class.simpleName}")
        else inner.writeObject(value.toList())
}
