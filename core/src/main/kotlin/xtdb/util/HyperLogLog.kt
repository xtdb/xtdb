@file:JvmName("HyperLogLog")
package xtdb.util

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import xtdb.arrow.VectorReader
import java.nio.ByteBuffer
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.pow

typealias HLL = ByteBuffer

// http://dimacs.rutgers.edu/~graham/pubs/papers/cacm-sketch.pdf
// http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf

const val DEFAULT_BUFFER_SIZE: Int = Integer.BYTES * 1024

operator fun HLL.plus(hash: Int): HLL {
    val m = this.capacity() / Integer.BYTES
    val b = Integer.numberOfTrailingZeros(m)


    val j = (hash ushr (Integer.SIZE - b)) and (m - 1)
    val w = hash and ((1 shl (Integer.SIZE - b)) - 1)

    val position = j * Integer.BYTES
    val currentValue = this.getInt(position)
    val newValue = max(currentValue, (Integer.numberOfLeadingZeros(w) + 1) - b)

    this.putInt(position, newValue)

    return this
}

fun HLL.add(vec: VectorReader, idx: Int, hasher: Hasher = Hasher.Xx()) = this + vec.hashCode(idx, hasher)

private val TWO_TO_32 = 1L shl 32
private val LARGEST_UNSIGNED_INT : Int = (TWO_TO_32 - 1).toInt()

fun HLL.estimate(): Double {
    val m = this.capacity() / Integer.BYTES

    var acc = 0.0
    for (i in 0 until this.capacity() step Integer.BYTES) {
        acc += 2.0.pow(-this.getInt(i))
    }

    val z = 1.0 / acc
    val alpha = 0.7213 / (1.0 + 1.079 / m)
    val e = alpha * m.toDouble().pow(2.0) * z

    // Small range correction
    if (e <= 2.5 * m) {
        var v = 0L
        for (i in 0 until this.capacity() step Integer.BYTES) {
            if (this.getInt(i) == 0) {
                v++
            }
        }

        return if (v == 0L) {
            e
        } else {
            m * ln(m.toDouble() / v)
        }
    }

    if (e > LARGEST_UNSIGNED_INT / 30.0 ) {
        return TWO_TO_32 * ln(1.0 - e / LARGEST_UNSIGNED_INT)
    }

    return e
}

fun HLL.combine(hll: HLL): HLL {
    require(this.capacity() == hll.capacity()) { "HyperLogLog buffers must have the same capacity" }

    val result = ByteBuffer.allocate(this.capacity())
    for (i in 0 until hll.capacity() step Integer.BYTES) {
        result.putInt(i, max(this.getInt(i), hll.getInt(i)))
    }
    return result
}

fun HLL.estimateUnion(hll: HLL): Double = combine(hll).estimate()

fun HLL.estimateIntersection(hll: HLL): Double = estimate() + hll.estimate() - estimateUnion(hll)

fun HLL.estimateDifference(hll: HLL): Double = estimate() - estimateIntersection(hll)

fun createHLL(bufferSize: Int = DEFAULT_BUFFER_SIZE): HLL = ByteBuffer.allocate(bufferSize)

fun toHLL(bytes: ByteArray): HLL = ByteBuffer.wrap(bytes)