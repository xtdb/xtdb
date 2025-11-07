package xtdb.util

import org.apache.arrow.memory.ArrowBuf
import org.apache.commons.codec.digest.MurmurHash3
import xtdb.arrow.ArrowUtil.toByteArray
import java.lang.Double.doubleToRawLongBits
import java.lang.Integer.rotateLeft
import java.nio.ByteBuffer

abstract class Hasher {
    fun hash(value: Double): Int = hash(doubleToRawLongBits(value))
    open fun hash(value: Long): Int = hash(longToBytes(value))
    open fun hash(value: ByteBuffer): Int = hash(value.toByteArray())
    open fun hash(buf: ArrowBuf, offset: Int, length: Int): Int = hash(buf.toByteArray(offset, length))

    abstract fun hash(value: ByteArray): Int

    companion object {
        private fun longToBytes(value: Long) =
            ByteArray(Long.SIZE_BYTES) { (value ushr ((7 - it) * 8)).toByte() }
    }

    class Xx(val seed: Int = 0) : Hasher() {
        companion object {
            // XXHash32 constants
            private const val PRIME32_1 = -1640531535  // 0x9E3779B1
            private const val PRIME32_2 = -2048144777  // 0x85EBCA77
            private const val PRIME32_3 = -1028477379  // 0xC2B2AE3D
            private const val PRIME32_4 = 668265263    // 0x27D4EB2F
            private const val PRIME32_5 = 374761393    // 0x165667B1
        }

        private fun processLane(acc: Int, lane: Int): Int = rotateLeft(acc + lane * PRIME32_2, 13) * PRIME32_1

        private fun avalanche(h32: Int): Int {
            var h = h32
            h = h xor (h ushr 15)
            h *= PRIME32_2
            h = h xor (h ushr 13)
            h *= PRIME32_3
            h = h xor (h ushr 16)
            return h
        }

        override fun hash(value: Long): Int {
            var h32 = seed + PRIME32_5 + 8

            // Extract bytes in big-endian order and process as little-endian ints
            val b0 = (value ushr 56).toByte()
            val b1 = (value ushr 48).toByte()
            val b2 = (value ushr 40).toByte()
            val b3 = (value ushr 32).toByte()
            val b4 = (value ushr 24).toByte()
            val b5 = (value ushr 16).toByte()
            val b6 = (value ushr 8).toByte()
            val b7 = value.toByte()

            val v1 = (b0.toInt() and 0xFF) or
                    ((b1.toInt() and 0xFF) shl 8) or
                    ((b2.toInt() and 0xFF) shl 16) or
                    ((b3.toInt() and 0xFF) shl 24)

            val v2 = (b4.toInt() and 0xFF) or
                    ((b5.toInt() and 0xFF) shl 8) or
                    ((b6.toInt() and 0xFF) shl 16) or
                    ((b7.toInt() and 0xFF) shl 24)

            h32 += v1 * PRIME32_3
            h32 = rotateLeft(h32, 17) * PRIME32_4
            h32 += v2 * PRIME32_3
            h32 = rotateLeft(h32, 17) * PRIME32_4
            return avalanche(h32)
        }

        private sealed interface Bytes {
            fun getByte(index: Int): Byte
            fun getInt(index: Int): Int

            class Array(val data: ByteArray) : Bytes {
                override fun getByte(index: Int) = data[index]

                override fun getInt(index: Int): Int =
                    (data[index].toInt() and 0xFF)
                        .or((data[index + 1].toInt() and 0xFF) shl 8)
                        .or((data[index + 2].toInt() and 0xFF) shl 16)
                        .or((data[index + 3].toInt() and 0xFF) shl 24)
            }

            class Buf(val buf: ArrowBuf) : Bytes {
                override fun getByte(index: Int) = buf.getByte(index.toLong())

                override fun getInt(index: Int) = buf.getInt(index.toLong())
            }
        }

        private fun hash(data: Bytes, offset: Int, length: Int): Int {
            var h32: Int
            var index = offset
            val end = offset + length

            if (length >= 16) {
                var v1 = seed + PRIME32_1 + PRIME32_2
                var v2 = seed + PRIME32_2
                var v3 = seed
                var v4 = seed - PRIME32_1

                val limit = end - 16
                while (index <= limit) {
                    v1 = processLane(v1, data.getInt(index))
                    index += 4
                    v2 = processLane(v2, data.getInt(index))
                    index += 4
                    v3 = processLane(v3, data.getInt(index))
                    index += 4
                    v4 = processLane(v4, data.getInt(index))
                    index += 4
                }

                h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18)
            } else {
                h32 = seed + PRIME32_5
            }

            h32 += length

            while (index <= end - 4) {
                h32 += data.getInt(index) * PRIME32_3
                h32 = rotateLeft(h32, 17) * PRIME32_4
                index += 4
            }

            while (index < end) {
                h32 += (data.getByte(index).toInt() and 0xFF) * PRIME32_5
                h32 = rotateLeft(h32, 11) * PRIME32_1
                index++
            }

            return avalanche(h32)
        }

        override fun hash(buf: ArrowBuf, offset: Int, length: Int) = hash(Bytes.Buf(buf), offset, length)

        fun hash(data: ByteArray, offset: Int, length: Int): Int = hash(Bytes.Array(data), offset, length)

        override fun hash(value: ByteArray): Int = hash(value, 0, value.size)

        override fun hash(value: ByteBuffer): Int {
            val buf = value.duplicate()
            return if (buf.hasArray()) {
                hash(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining())
            } else {
                hash(value.toByteArray())
            }
        }
    }

    class Murmur3 : Hasher() {
        override fun hash(value: ByteArray) = MurmurHash3.hash32x86(value)
    }
}