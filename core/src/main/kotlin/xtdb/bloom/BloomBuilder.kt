package xtdb.bloom

import com.carrotsearch.hppc.IntArrayList
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.util.Hasher

class BloomBuilder (val col: VectorReader, val k: Int, val mask: Int) {
    private val buf = IntArrayList((col.valueCount * k))
    private val xxHasher = Hasher.Xx()
    private val murmur3Hasher = Hasher.Murmur3()

    constructor(col: VectorReader) : this(col, BLOOM_K, BLOOM_BIT_MASK)

    fun add(idx: Int) {
        val hash1 = col.hashCode(idx, xxHasher)
        val hash2 = col.hashCode(idx, murmur3Hasher)

        for (n in 0 until k) {
            buf.add(mask and (hash1 + (hash2 * n)))
        }
    }

    fun build(): RoaringBitmap {
        return RoaringBitmap.bitmapOfUnordered(*buf.toArray())
    }
}