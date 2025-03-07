package xtdb.bloom

import com.carrotsearch.hppc.IntArrayList
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.util.Hasher

interface BloomBuilder {
    fun add(idx: Int)
    fun build(): RoaringBitmap

    companion object {
        @JvmStatic
        fun create(col: VectorReader): BloomBuilder {
            return create(col, BloomUtils.BLOOM_K, BloomUtils.BLOOM_BIT_MASK)
        }

        @JvmStatic
        fun create(col: VectorReader, k: Long, mask: Long): BloomBuilder {
            val buf = IntArrayList((col.valueCount * k).toInt())
            val xxHasher = Hasher.Xx()
            val murmur3Hasher = Hasher.Murmur3()

            return object : BloomBuilder {
                override fun add(idx: Int) {
                    val hash1 = col.hashCode(idx, xxHasher)
                    val hash2 = col.hashCode(idx, murmur3Hasher)

                    for (n in 0 until k) {
                        // The toInt() call truncates
                        buf.add((mask and (hash1 + (hash2 * n))).toInt())
                    }
                }

                override fun build(): RoaringBitmap {
                    return RoaringBitmap.bitmapOfUnordered(*buf.toArray())
                }
            }
        }
    }
}