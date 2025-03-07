package xtdb.bloom

import org.apache.arrow.memory.util.ArrowBufPointer
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.arrow.VectorWriter
import xtdb.util.Hasher
import java.nio.ByteBuffer

object BloomUtils {
    const val BLOOM_BITS = 1L shl 20
    const val BLOOM_BIT_MASK = BLOOM_BITS - 1
    const val BLOOM_K = 3L

    @JvmStatic
    fun bloomToBitmap(bloomRdr: VectorReader, idx: Int): ImmutableRoaringBitmap {
        val pointer = ArrowBufPointer().apply {
            bloomRdr.getPointer(idx, this)
        }
        val nioBuffer = pointer.getBuf()!!.nioBuffer(pointer.getOffset(), pointer.getLength().toInt())
        return ImmutableRoaringBitmap(nioBuffer)
    }

    @JvmStatic
    fun bloomContains(bloomRdr: VectorReader, idx: Int, hashes: IntArray) =
        bloomContains(bloomToBitmap(bloomRdr, idx), hashes)

    @JvmStatic
    fun bloomContains(bloom: ImmutableRoaringBitmap, hashes: IntArray): Boolean {
        for (i in hashes.indices) {
            if (!bloom.contains(hashes[i])) {
                return false
            }
        }
        return true
    }

    @JvmStatic
    fun bloomHashes(col: VectorReader, idx: Int): IntArray {
        return bloomHashes(col, idx, BLOOM_K, BLOOM_BIT_MASK)
    }

    // Cassandra-style hashes:
    // https://www.researchgate.net/publication/220770131_Less_Hashing_Same_Performance_Building_a_Better_Bloom_Filter

    @JvmStatic
    fun bloomHashes(col: VectorReader, idx: Int, k: Long, mask: Long): IntArray {
        val hash1 = col.hashCode(idx, Hasher.Xx())
        val hash2 = col.hashCode(idx, Hasher.Murmur3())
        val acc = IntArray(k.toInt())
        for (n in 0 until k) {
            acc[n.toInt()] = (mask and (hash1 + hash2 * n)).toInt()
        }
        return acc
    }

    @JvmStatic
    fun ByteBufferToRoaringBloom(buf: ByteBuffer): RoaringBitmap {
        val bloom = RoaringBitmap()
        bloom.deserialize(buf)
        return bloom
    }

    fun roaringBloomToByteBuffer(bloom: RoaringBitmap): ByteBuffer {
        val buf = ByteBuffer.allocate(bloom.serializedSizeInBytes())
        bloom.serialize(buf)
        buf.clear()
        return buf
    }

    @JvmStatic
    fun writeBloom(bloomWtr: VectorWriter, col: VectorReader) {
        val bloomBuilder = BloomBuilder.create(col)
        for (idx in 0 until col.valueCount) {
            if (!col.isNull(idx)) {
                bloomBuilder.add(idx)
            }
        }

        val bloom = bloomBuilder.build()
        bloomWtr.writeBytes(roaringBloomToByteBuffer(bloom))
    }
}