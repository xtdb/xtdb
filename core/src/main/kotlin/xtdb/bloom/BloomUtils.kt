@file:JvmName("BloomUtils")

package xtdb.bloom

import org.apache.arrow.memory.util.ArrowBufPointer
import org.roaringbitmap.ImmutableBitmapDataProvider
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.util.Hasher
import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN

const val BLOOM_BITS = 24
const val BLOOM_K = 3

typealias BloomFilter = ImmutableBitmapDataProvider

fun bloomToBitmap(bloomRdr: VectorReader, idx: Int): BloomFilter {
    val pointer = ArrowBufPointer().apply {
        bloomRdr.getPointer(idx, this)
    }
    val nioBuffer = pointer.buf!!.nioBuffer(pointer.offset, pointer.length.toInt()).order(LITTLE_ENDIAN)
    return ImmutableRoaringBitmap(nioBuffer)
}

fun bloomContains(bloomRdr: VectorReader, idx: Int, hashes: IntArray) = bloomToBitmap(bloomRdr, idx).contains(hashes)

fun BloomFilter.contains(hashes: IntArray) = hashes.all { contains(it) }

// Cassandra-style hashes:
// https://www.researchgate.net/publication/220770131_Less_Hashing_Same_Performance_Building_a_Better_Bloom_Filter

@JvmOverloads
fun bloomHashes(col: VectorReader, idx: Int, k: Int = BLOOM_K, bits: Int = BLOOM_BITS): IntArray {
    val mask = (1 shl bits) - 1
    val hash1 = col.hashCode(idx, Hasher.Xx())
    val hash2 = col.hashCode(idx, Hasher.Murmur3())
    return IntArray(k) { n -> (mask and (hash1 + hash2 * n)) }
}

fun BloomFilter.toByteBuffer(): ByteBuffer =
    ByteBuffer.allocate(this.serializedSizeInBytes()).also {
        this.serialize(it)
        it.clear()
    }

fun writeBloom(bloomWtr: VectorWriter, col: VectorReader) {
    val bloomBuilder = BloomBuilder()
    for (idx in 0 until col.valueCount) {
        if (!col.isNull(idx)) {
            bloomBuilder.add(col, idx)
        }
    }

    val bloom = bloomBuilder.build()
    bloomWtr.writeBytes(bloom.toByteBuffer())
}