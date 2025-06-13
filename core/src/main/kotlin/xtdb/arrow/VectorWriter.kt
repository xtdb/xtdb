package xtdb.arrow

import clojure.lang.IExceptionInfo
import clojure.lang.IPersistentMap
import clojure.lang.PersistentArrayMap
import org.apache.arrow.vector.types.pojo.FieldType
import xtdb.asKeyword

internal data class InvalidWriteObjectException(val fieldType: FieldType, val obj: Any?) :
    IllegalArgumentException("invalid writeObject"), IExceptionInfo {
    override fun getData(): IPersistentMap =
        PersistentArrayMap.create(mapOf("field-type".asKeyword to fieldType, "obj".asKeyword to obj))
}

internal data class InvalidCopySourceException(val src: FieldType, val dest: FieldType) :
    IllegalArgumentException("illegal copy src vector")

interface VectorWriter : VectorReader, ValueWriter, AutoCloseable {

    override val nullable: Boolean

    fun writeUndefined()

    fun writeValue(v: ValueReader) = if (v.isNull) writeNull() else writeValue0(v)
    fun writeValue0(v: ValueReader)

    override fun vectorForOrNull(name: String): VectorWriter? = unsupported("vectorFor")

    override fun vectorFor(name: String) = vectorForOrNull(name) ?: error("missing vector: $name")
    override fun get(name: String) = vectorFor(name)

    /**
     * @return a vector with the given name, creating/promoting if necessary
     */
    fun vectorFor(name: String, fieldType: FieldType): VectorWriter = unsupported("vectorFor")

    fun endStruct(): Unit = unsupported("endStruct")

    override val listElements: VectorWriter get() = unsupported("listElements")
    fun getListElements(fieldType: FieldType): VectorWriter = unsupported("elementWriter")
    fun endList(): Unit = unsupported("endList")

    override val mapKeys: VectorWriter get() = unsupported("mapKeys")
    fun getMapKeys(fieldType: FieldType): VectorWriter = unsupported("mapKeys")
    override val mapValues: VectorWriter get() = unsupported("mapValues")
    fun getMapValues(fieldType: FieldType): VectorWriter = unsupported("mapValueWriter")

    fun rowCopier0(src: VectorReader): RowCopier

    fun clear()

    fun writeAll(vals: Iterable<Any?>) = apply { vals.forEach { writeObject(it) } }
}
