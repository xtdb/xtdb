package xtdb.arrow

import clojure.lang.Keyword
import xtdb.api.query.IKeyFn
import xtdb.vector.extensions.KeywordType

class KeywordVector(override val inner: Utf8Vector): ExtensionVector(KeywordType) {

    override fun getObject0(idx: Int, keyFn: IKeyFn<*>) = Keyword.intern(inner.getObject0(idx, keyFn))

    override fun writeObject0(value: Any) = when(value) {
        is Keyword -> inner.writeObject(value.sym.toString())
        else -> TODO("promotion")
    }
}