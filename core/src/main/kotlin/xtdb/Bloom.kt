package xtdb

import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import xtdb.arrow.VectorReader

typealias BloomFilter = ImmutableRoaringBitmap

fun readBloom(bloomReader: VectorReader, idx: Int): BloomFilter {
    val pointer = bloomReader.getPointer(idx)
    return BloomFilter(pointer.buf!!.nioBuffer(pointer.offset, pointer.length.toInt()))
}