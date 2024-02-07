package xtdb.trie

import clojure.lang.Keyword
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.bitemporal.IPolygonReader
import xtdb.trie.HashTrie.Companion.compareToPath
import xtdb.vector.IVectorReader
import xtdb.vector.RelationReader

class EventRowPointer(val relReader: RelationReader, path: ByteArray) : IPolygonReader {
    private val iidReader: IVectorReader = relReader.readerForName("xt\$iid")
    private val sysFromReader: IVectorReader = relReader.readerForName("xt\$system_from")
    private val opReader: IVectorReader = relReader.readerForName("op")

    private val validTimesReader: IVectorReader = relReader.readerForName("xt\$valid_times")
    private val validTimeReader: IVectorReader = validTimesReader.listElementReader()
    private val systemTimeCeilingsReader: IVectorReader = relReader.readerForName("xt\$system_time_ceilings")
    private val systemTimeCeilingReader: IVectorReader = systemTimeCeilingsReader.listElementReader()

    var index: Int
        private set

    init {
        var left = 0
        var right = relReader.rowCount()
        var mid: Int
        while (left < right) {
            mid = (left + right) / 2
            if (compareToPath(iidReader.getPointer(mid), path) < 0) left = mid + 1
            else right = mid
        }
        this.index = left
    }

    fun nextIndex() = ++index

    fun getIidPointer(reuse: ArrowBufPointer) = iidReader.getPointer(index, reuse)

    val systemFrom get() = sysFromReader.getLong(index)

    override val validTimeRangeCount get() = systemTimeCeilingsReader.getListCount(index)

    override fun getValidFrom(rangeIdx: Int) =
        validTimeReader.getLong(validTimesReader.getListStartIndex(index) + rangeIdx)

    override fun getValidTo(rangeIdx: Int): Long =
        validTimeReader.getLong(validTimesReader.getListStartIndex(index) + rangeIdx + 1)

    override fun getSystemTo(rangeIdx: Int): Long =
        systemTimeCeilingReader.getLong(systemTimeCeilingsReader.getListStartIndex(index) + rangeIdx)

    val op: Keyword get() = opReader.getLeg(index)

    fun isValid(reuse: ArrowBufPointer, path: ByteArray) =
        index < relReader.rowCount() && compareToPath(getIidPointer(reuse), path) <= 0

    companion object {
        @JvmStatic
        fun comparator(): Comparator<in EventRowPointer> {
            val leftCmp = ArrowBufPointer()
            val rightCmp = ArrowBufPointer()

            return Comparator { l, r ->
                val cmp = l.getIidPointer(leftCmp).compareTo(r.getIidPointer(rightCmp))
                if (cmp != 0) cmp else r.systemFrom compareTo l.systemFrom
            }
        }
    }
}
