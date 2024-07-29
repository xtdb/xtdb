package xtdb.arrow

import org.apache.arrow.vector.types.pojo.Field

interface VectorReader : AutoCloseable {
    val name: String
    val valueCount: Int

    val nullable: Boolean
    val arrowField: Field

    fun isNull(idx: Int): Boolean
    fun getBoolean(idx: Int): Boolean = unsupported("getBoolean")
    fun getByte(idx: Int): Byte = unsupported("getByte")
    fun getShort(idx: Int): Short = unsupported("getShort")
    fun getInt(idx: Int): Int = unsupported("getInt")
    fun getLong(idx: Int): Long = unsupported("getLong")
    fun getFloat(idx: Int): Float = unsupported("getFloat")
    fun getDouble(idx: Int): Double = unsupported("getDouble")
    fun getBytes(idx: Int): ByteArray = unsupported("getBytes")
    fun getObject(idx: Int): Any?

    fun elementReader(): VectorReader = unsupported("elementReader")
    fun getListStartIndex(idx: Int): Int = unsupported("getListStartIndex")
    fun getListCount(idx: Int): Int = unsupported("getListCount")

    fun mapKeyReader(): VectorReader = unsupported("mapKeyReader")
    fun mapValueReader(): VectorReader = unsupported("mapValueReader")

    fun keyReader(name: String): VectorReader = unsupported("keyReader")

    fun legReader(name: String): VectorReader = unsupported("legWriter")
    fun getLeg(idx: Int): String = unsupported("getLeg")

    fun toList(): List<Any?>
}