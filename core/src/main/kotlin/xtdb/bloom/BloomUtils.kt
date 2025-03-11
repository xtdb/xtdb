@file:JvmName("BloomUtils")

package xtdb.bloom

import org.apache.arrow.memory.util.ArrowBufPointer
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.util.Hasher
import java.nio.ByteBuffer

const val BLOOM_BITS = 1 shl 20
const val BLOOM_BIT_MASK = BLOOM_BITS - 1
const val BLOOM_K = 3

fun bloomToBitmap(bloomRdr: VectorReader, idx: Int): ImmutableRoaringBitmap {
    val pointer = ArrowBufPointer().apply {
        bloomRdr.getPointer(idx, this)
    }
    val nioBuffer = pointer.buf!!.nioBuffer(pointer.offset, pointer.length.toInt())
    return ImmutableRoaringBitmap(nioBuffer)
}

fun bloomContains(bloomRdr: VectorReader, idx: Int, hashes: IntArray) = bloomToBitmap(bloomRdr, idx).contains(hashes)

fun ImmutableRoaringBitmap.contains(hashes: IntArray) = hashes.all { contains(it) }

// Cassandra-style hashes:
// https://www.researchgate.net/publication/220770131_Less_Hashing_Same_Performance_Building_a_Better_Bloom_Filter

@JvmOverloads
fun bloomHashes(col: VectorReader, idx: Int, k: Int = BLOOM_K, mask: Int = BLOOM_BIT_MASK): IntArray {
    val hash1 = col.hashCode(idx, Hasher.Xx())
    val hash2 = col.hashCode(idx, Hasher.Murmur3())
    return IntArray(k) { n -> (mask and (hash1 + hash2 * n)) }
}

fun RoaringBitmap.toByteBuffer(): ByteBuffer =
    ByteBuffer.allocate(this.serializedSizeInBytes()).also {
        this.serialize(it)
        it.clear()
    }

fun writeBloom(bloomWtr: VectorWriter, col: VectorReader) {
    val bloomBuilder = BloomBuilder(col)
    for (idx in 0 until col.valueCount) {
        if (!col.isNull(idx)) {
            bloomBuilder.add(idx)
        }
    }

    val bloom = bloomBuilder.build()
    bloomWtr.writeBytes(bloom.toByteBuffer())
}