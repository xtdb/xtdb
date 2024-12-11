package xtdb.arrow

import xtdb.api.query.IKeyFn
import xtdb.vector.extensions.SetType

class SetVector(override val inner: ListVector) : ExtensionVector(SetType) {

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = inner.getObject0(idx, keyFn).toSet()

    override fun writeObject0(value: Any) =
        if (value !is Set<*>) TODO("promotion ${value::class.simpleName}")
        else inner.writeObject(value.toList())
}
