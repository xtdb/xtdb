package xtdb.arrow

import java.nio.ByteBuffer

interface ValueReader {
    val leg: String?

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
