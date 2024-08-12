package xtdb.arrow

interface ListValueReader {
    fun size(): Int

    fun nth(idx: Int): ValueReader
}
