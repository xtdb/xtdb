package xtdb.arrow

interface LongLongVectorReader : VectorReader {
    fun getLongLongHigh(idx: Int): Long
    fun getLongLongLow(idx: Int): Long
}