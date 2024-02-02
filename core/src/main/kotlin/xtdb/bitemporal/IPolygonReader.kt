package xtdb.bitemporal

interface IPolygonReader {
    val validTimeRangeCount: Int

    fun getValidFrom(rangeIdx: Int): Long

    fun getValidTo(rangeIdx: Int): Long

    fun getSystemTo(rangeIdx: Int): Long
}
