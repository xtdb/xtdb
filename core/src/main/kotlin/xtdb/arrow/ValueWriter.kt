package xtdb.arrow

import java.nio.ByteBuffer

interface ValueWriter {
    fun writeNull()

    fun writeBoolean(v: Boolean): Unit = unsupported("writeBoolean")
    fun writeByte(v: Byte): Unit = unsupported("writeByte")
    fun writeShort(v: Short): Unit = unsupported("writeShort")
    fun writeInt(v: Int): Unit = unsupported("writeInt")
    fun writeLong(v: Long): Unit = unsupported("writeLong")
    fun writeFloat(v: Float): Unit = unsupported("writeFloat")
    fun writeDouble(v: Double): Unit = unsupported("writeDouble")
    fun writeBytes(v: ByteBuffer): Unit = unsupported("writeBytes")
    fun writeObject(obj: Any?)

    fun legWriter(leg: String): ValueWriter
}
