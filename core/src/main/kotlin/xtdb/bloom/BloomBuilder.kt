package xtdb.bloom

import com.carrotsearch.hppc.IntArrayList
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.VectorReader
import xtdb.arrow.metadata.MetadataFlavour
import xtdb.util.Hasher
import java.nio.ByteBuffer

class BloomBuilder(private val k: Int = BLOOM_K, bits: Int = BLOOM_BITS) {
    private val mask = (1 shl bits) - 1
    private val buf = IntArrayList()
    private val xxHasher = Hasher.Xx()
    private val murmur3Hasher = Hasher.Murmur3()

    fun add(hash1: Int, hash2: Int) = repeat(k) { n -> buf.add(mask and (hash1 + (hash2 * n))) }
    fun add(buf: ByteBuffer) = add(xxHasher.hash(buf), murmur3Hasher.hash(buf))

    fun add(col: MetadataFlavour.Bytes, idx: Int) = add(col.getBytes(idx))
    fun add(col: MetadataFlavour.Bytes) = repeat(col.valueCount) { add(col, it) }

    fun add(col: VectorReader, idx: Int) = add(col.hashCode(idx, xxHasher), col.hashCode(idx, murmur3Hasher))
    fun add(col: VectorReader) = repeat(col.valueCount) { add(col, it) }

    fun build(): RoaringBitmap = RoaringBitmap.bitmapOfUnordered(*buf.toArray())
}