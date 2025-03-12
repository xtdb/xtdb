package xtdb.util

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import xtdb.arrow.VectorReader
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.pow

typealias HLL = ByteBuf

object HyperLogLog {

    const val DEFAULT_BUFFER_SIZE: Int = Integer.BYTES * 1024

    /**
     * Add a value to the HyperLogLog counter
     * @param hll The ByteBuf containing HLL registers
     * @param hash The value to add
     * @return The same ByteBuf after modification
     */
    fun add(hll: ByteBuf, hash: Int): ByteBuf {
        val m = hll.capacity() / Integer.BYTES
        val b = Integer.numberOfTrailingZeros(m)


        val j = (hash ushr (Integer.SIZE - b)) and (m - 1)
        val w = hash and ((1 shl (Integer.SIZE - b)) - 1)

        val position = j * Integer.BYTES
        val currentValue = hll.getInt(position)
        val newValue = max(currentValue, (Integer.numberOfLeadingZeros(w) + 1) - b)

        hll.setInt(position, newValue)

        return hll
    }

    @JvmStatic
    @JvmOverloads
    fun add(hll: ByteBuf, vec: VectorReader, idx: Int, hasher: Hasher = Hasher.Xx()) = add(hll, vec.hashCode(idx, hasher))

    private val twoTo32 = Integer.toUnsignedLong(-1)

    /**
     * Estimate the cardinality of the set represented by this HyperLogLog
     * @param hll The ByteBuf containing HLL registers
     * @return The estimated cardinality
     */
    @JvmStatic
    fun estimate(hll: ByteBuf): Double {
        val m = hll.capacity() / Integer.BYTES

        var acc = 0.0
        for (i in 0 until hll.capacity() step Integer.BYTES) {
            acc += 2.0.pow(-hll.getInt(i))
        }

        val z = 1.0 / acc
        val alpha = 0.7213 / (1.0 + 1.079 / m)
        val e = alpha * m.toDouble().pow(2.0) * z

        // Small range correction
        if (e <= 2.5 * m) {
            var v = 0L
            for (i in 0 until hll.capacity() step Integer.BYTES) {
                if (hll.getInt(i) == 0) {
                    v++
                }
            }

            return if (v == 0L) {
                e
            } else {
                m * ln(m.toDouble() / v)
            }
        }

        if (e > twoTo32 / 30) {
            return -twoTo32 * ln(1.0 - e / twoTo32)
        }

        return e
    }

    /**
     * Create a new HyperLogLog ByteBuf with the combined maximum of two HLLs
     * @param hllA First HLL buffer
     * @param hllB Second HLL buffer
     * @return A new ByteBuf containing the combined HLL
     */
    fun combine(hllA: ByteBuf, hllB: ByteBuf): ByteBuf {
        require(hllA.capacity() == hllB.capacity()) { "HyperLogLog buffers must have the same capacity" }

        val result = Unpooled.buffer(hllA.capacity())
        for (i in 0 until hllA.capacity() step Integer.BYTES) {
            result.setInt(i, max(hllA.getInt(i), hllB.getInt(i)))
        }
        return result
    }

    /**
     * Estimate the cardinality of the union of two sets
     * @param hllA First HLL buffer
     * @param hllB Second HLL buffer
     * @return The estimated cardinality of the union
     */
    fun estimateUnion(hllA: ByteBuf, hllB: ByteBuf): Double {
        return estimate(combine(hllA, hllB))
    }

    /**
     * Estimate the cardinality of the intersection of two sets
     * @param hllA First HLL buffer
     * @param hllB Second HLL buffer
     * @return The estimated cardinality of the intersection
     */
    fun estimateIntersection(hllA: ByteBuf, hllB: ByteBuf): Double {
        return estimate(hllA) + estimate(hllB) - estimateUnion(hllA, hllB)
    }

    /**
     * Estimate the cardinality of the difference between two sets (A - B)
     * @param hllA First HLL buffer
     * @param hllB Second HLL buffer
     * @return The estimated cardinality of the difference
     */
    fun estimateDifference(hllA: ByteBuf, hllB: ByteBuf): Double {
        return estimate(hllA) - estimateIntersection(hllA, hllB)
    }

    @JvmStatic
    fun createHLL(bufferSize: Int = DEFAULT_BUFFER_SIZE) = Unpooled.buffer(bufferSize)
    @JvmStatic
    fun toHLL(bytes: ByteArray) = Unpooled.wrappedBuffer(bytes)
}