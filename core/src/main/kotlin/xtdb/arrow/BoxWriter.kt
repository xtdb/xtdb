package xtdb.arrow

import java.nio.ByteBuffer

internal abstract class BoxWriter : ValueWriter {
    abstract fun box(): ValueBox

    override fun writeNull() {
        box().writeNull()
    }

    override fun writeBoolean(v: Boolean) {
        box().writeBoolean(v)
    }

    override fun writeByte(v: Byte) {
        box().writeByte(v)
    }

    override fun writeShort(v: Short) {
        box().writeShort(v)
    }

    override fun writeInt(v: Int) {
        box().writeInt(v)
    }

    override fun writeLong(v: Long) {
        box().writeLong(v)
    }

    override fun writeFloat(v: Float) {
        box().writeFloat(v)
    }

    override fun writeDouble(v: Double) {
        box().writeDouble(v)
    }

    override fun writeBytes(v: ByteBuffer) {
        box().writeBytes(v)
    }

    override fun writeObject(obj: Any?) {
        box().writeObject(obj)
    }

    fun legWriter(leg: String): ValueWriter {
        return box().legWriter(leg)
    }
}
