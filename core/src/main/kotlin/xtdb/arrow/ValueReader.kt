package xtdb.arrow

import java.nio.ByteBuffer

interface ValueReader {
    val leg: String? get() = unsupported("leg")

    val isNull: Boolean get() = unsupported("isNull")

    fun readBoolean(): Boolean = unsupported("readBoolean")
    fun readByte(): Byte = unsupported("readByte")
    fun readShort(): Short = unsupported("readShort")
    fun readInt(): Int = unsupported("readInt")
    fun readLong(): Long = unsupported("readLong")

    fun readFloat(): Float = unsupported("readFloat")
    fun readDouble(): Double = unsupported("readDouble")

    fun readBytes(): ByteBuffer = unsupported("readBytes")
    fun readObject(): Any? = unsupported("readObject")
}
