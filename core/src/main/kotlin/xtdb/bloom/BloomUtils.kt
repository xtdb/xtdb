@file:JvmName("BloomUtils")

package xtdb.bloom

import org.apache.arrow.memory.util.ArrowBufPointer
import org.roaringbitmap.ImmutableBitmapDataProvider
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import xtdb.arrow.ArrowUtil.toByteBuffer
import xtdb.arrow.IntVector
import xtdb.arrow.VectorReader
import xtdb.util.Hasher
import java.nio.ByteOrder.LITTLE_ENDIAN

const val BLOOM_BITS = 22
const val BLOOM_K = 5

typealias BloomFilter = ImmutableBitmapDataProvider

fun bloomToBitmap(bloomRdr: VectorReader, idx: Int): BloomFilter {
    val pointer = ArrowBufPointer().apply {
        bloomRdr.getPointer(idx, this)
    }
    val nioBuffer = pointer.buf!!.toByteBuffer(pointer.offset, pointer.length).order(LITTLE_ENDIAN)
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

/**
 * The positions [bloomHashes] gives each row, from that row's [xxHashes] and [murmur3Hashes] entries, [BLOOM_K] per row in row order.
 */
fun bloomPositions(xxHashes: IntVector, murmur3Hashes: IntVector): IntArray {
    val mask = (1 shl BLOOM_BITS) - 1
    val positions = IntArray(xxHashes.valueCount * BLOOM_K)

    repeat(xxHashes.valueCount) { row ->
        val hash1 = xxHashes.getInt(row)
        val hash2 = murmur3Hashes.getInt(row)
        repeat(BLOOM_K) { n -> positions[row * BLOOM_K + n] = mask and (hash1 + hash2 * n) }
    }

    return positions
}
