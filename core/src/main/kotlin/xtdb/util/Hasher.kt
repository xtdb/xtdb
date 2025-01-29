package xtdb.util

import org.apache.commons.codec.digest.MurmurHash3
import org.apache.commons.codec.digest.XXHash32
import java.nio.ByteBuffer


abstract class Hasher {
    private val doubleByteBuf = ByteBuffer.allocate(Double.SIZE_BYTES)

    fun hash(value: Double): Int {
        doubleByteBuf.putDouble(0, value)
        return hash(doubleByteBuf.array())
    }

    private val longByteBuf = ByteBuffer.allocate(Long.SIZE_BYTES)

    fun hash(value: Long): Int {
        longByteBuf.putLong(0, value)
        return hash(longByteBuf.array())
    }

    fun hash(value: ByteBuffer) = hash(value.toByteArray())

    abstract fun hash(value: ByteArray): Int

    class Xx: Hasher() {
        private val hasher = XXHash32()

        override fun hash(value: ByteArray): Int {
            hasher.update(value)
            val res = hasher.value
            hasher.reset()
            return (res ushr 32).toInt() xor res.toInt()
        }
    }

    class Murmur3 : Hasher() {
        override fun hash(value: ByteArray) = MurmurHash3.hash32x86(value)
    }
}