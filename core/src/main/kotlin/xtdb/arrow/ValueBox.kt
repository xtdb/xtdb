package xtdb.arrow

import clojure.lang.Keyword
import java.nio.ByteBuffer

class ValueBox : ValueWriter, ValueReader {
    override var leg: String? = null
        private set

    private var prim: Long = 0
    private var obj: Any? = null

    override val isNull: Boolean
        get() = leg === NULL_LEG

    override fun readBoolean(): Boolean {
        return prim != 0L
    }

    override fun readByte(): Byte {
        return prim.toByte()
    }

    override fun readShort(): Short {
        return prim.toShort()
    }

    override fun readInt(): Int {
        return prim.toInt()
    }

    override fun readLong(): Long {
        return prim
    }

    override fun readFloat(): Float {
        return readDouble().toFloat()
    }

    override fun readDouble(): Double {
        return java.lang.Double.longBitsToDouble(prim)
    }

    override fun readBytes(): ByteBuffer {
        return (obj as ByteBuffer)
    }

    override fun readObject(): Any? {
        return obj
    }

    override fun writeNull() {
        leg = NULL_LEG
        obj = null
    }

    override fun writeBoolean(v: Boolean) {
        this.prim = (if (v) 1 else 0).toLong()
    }

    override fun writeByte(v: Byte) {
        this.prim = v.toLong()
    }

    override fun writeShort(v: Short) {
        this.prim = v.toLong()
    }

    override fun writeInt(v: Int) {
        this.prim = v.toLong()
    }

    override fun writeLong(v: Long) {
        this.prim = v
    }

    override fun writeFloat(v: Float) {
        writeDouble(v.toDouble())
    }

    override fun writeDouble(v: Double) {
        this.prim = java.lang.Double.doubleToLongBits(v)
    }

    override fun writeBytes(v: ByteBuffer) {
        this.obj = v
    }

    override fun writeObject(obj: Any?) {
        this.obj = obj
    }

    override fun legWriter(leg: String): ValueWriter {
        return object : BoxWriter() {
            override fun box(): ValueWriter {
                this@ValueBox.leg = leg
                return this@ValueBox
            }
        }
    }

    companion object {
        private val NULL_LEG = "null".intern()
    }
}
