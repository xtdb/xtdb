package xtdb.arrow

import java.lang.Double.*
import java.nio.ByteBuffer

private const val NULL_LEG = "null"

class ValueBox : ValueReader {
    override var leg: String? = null
        private set

    private var prim: Long = 0
    private var obj: Any? = null

    override val isNull: Boolean
        get() = leg === NULL_LEG

    override fun readBoolean() = prim != 0L
    override fun readByte() = prim.toByte()
    override fun readShort() = prim.toShort()
    override fun readInt() = prim.toInt()
    override fun readLong() = prim
    override fun readFloat() = readDouble().toFloat()
    override fun readDouble() = longBitsToDouble(prim)
    override fun readBytes(): ByteBuffer = obj as ByteBuffer
    override fun readObject(): Any? = obj

    fun writeNull() {
        leg = NULL_LEG
        obj = null
    }

    @JvmOverloads
    fun writeBoolean(leg: String? = null, v: Boolean) = writeLong(leg, (if (v) 1 else 0).toLong())

    @JvmOverloads
    fun writeByte(leg: String? = null, v: Byte) = writeLong(leg, v.toLong())

    @JvmOverloads
    fun writeShort(leg: String? = null, v: Short) = writeLong(leg, v.toLong())

    @JvmOverloads
    fun writeInt(leg: String? = null, v: Int) = writeLong(leg, v.toLong())

    @JvmOverloads
    fun writeLong(leg: String? = null, v: Long) {
        this.leg = leg
        this.prim = v
    }

    @JvmOverloads
    fun writeFloat(leg: String? = null, v: Float) = writeDouble(leg, v.toDouble())

    @JvmOverloads
    fun writeDouble(leg: String? = null, v: Double) {
        this.leg = leg
        this.prim = doubleToLongBits(v)
    }

    @JvmOverloads
    fun writeBytes(leg: String? = null, v: ByteBuffer) {
        this.leg = leg
        this.obj = v
    }

    @JvmOverloads
    fun writeObject(leg: String? = null, obj: Any?) {
        this.leg = leg
        this.obj = obj
    }
}
