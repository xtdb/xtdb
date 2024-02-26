package xtdb.vector

import clojure.lang.Keyword
import java.nio.ByteBuffer

interface IValueWriter {
    fun writeNull()
    fun writeBoolean(v: Boolean)
    fun writeByte(v: Byte)
    fun writeShort(v: Short)
    fun writeInt(v: Int)
    fun writeLong(v: Long)
    fun writeFloat(v: Float)
    fun writeDouble(v: Double)
    fun writeBytes(v: ByteBuffer)
    fun writeObject(v: Any?)

    fun legWriter(leg: Keyword): IValueWriter
}
