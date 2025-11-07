package xtdb.arrow

import java.nio.ByteBuffer

interface ValueReader {
    val leg: String? get() = unsupported("leg")

    val isNull: Boolean get() = unsupported("isNull")

    var pos: Int
        get() = unsupported("getPosition")
        set(_) = unsupported("setPosition")

    fun readBoolean(): Boolean = unsupported("readBoolean")
    fun readByte(): Byte = unsupported("readByte")
    fun readShort(): Short = unsupported("readShort")
    fun readInt(): Int = unsupported("readInt")
    fun readLong(): Long = unsupported("readLong")

    fun readFloat(): Float = unsupported("readFloat")
    fun readDouble(): Double = unsupported("readDouble")

    fun readBytes(): ByteBuffer = unsupported("readBytes")
    fun readObject(): Any? = unsupported("readObject")

    open class ForVector(private val vec: VectorReader) : ValueReader {
        override var pos: Int = 0

        override val leg get() = vec.getLeg(pos)

        override val isNull get() = vec.isNull(pos)

        override fun readBoolean() = vec.getBoolean(pos)
        override fun readByte() = vec.getByte(pos)
        override fun readShort() = vec.getShort(pos)
        override fun readInt() = vec.getInt(pos)
        override fun readLong() = vec.getLong(pos)
        override fun readFloat() = vec.getFloat(pos)
        override fun readDouble() = vec.getDouble(pos)
        override fun readBytes() = vec.getBytes(pos)
        override fun readObject() = vec.getObject(pos)
    }
}
