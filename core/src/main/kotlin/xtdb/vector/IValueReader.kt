package xtdb.vector

import clojure.lang.Keyword
import java.nio.ByteBuffer

interface IValueReader {
    val leg: Keyword?

    val isNull: Boolean

    fun readBoolean(): Boolean
    fun readByte(): Byte
    fun readShort(): Short
    fun readInt(): Int
    fun readLong(): Long

    fun readFloat(): Float
    fun readDouble(): Double

    fun readBytes(): ByteBuffer
    fun readObject(): Any?
}
