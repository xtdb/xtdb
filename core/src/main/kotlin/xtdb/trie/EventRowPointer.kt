package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.LongLongVectorReader
import xtdb.arrow.RelationReader
import java.lang.Long.compareUnsigned

class EventRowPointer(private val relReader: RelationReader, path: ByteArray) {
    private val iidReader = relReader["_iid"] as LongLongVectorReader

    private val sysFromReader = relReader["_system_from"]
    private val validFromReader = relReader["_valid_from"]
    private val validToReader = relReader["_valid_to"]

    private val opReader = relReader["op"]

    private var iidHigh: Long = -1
    private var iidLow: Long = -1

    private fun indexOf(path: ByteArray): Int {
        var left = 0
        var right = relReader.rowCount
        var mid: Int
        while (left < right) {
            mid = (left + right) / 2
            if (Bucketer.DEFAULT.compareToPath(iidReader.getPointer(mid), path) < 0) left = mid + 1
            else right = mid
        }

        return left
    }

    var index: Int = -1
        private set(value) {
            field = value
            if (value < relReader.rowCount) {
                iidHigh = iidReader.getLongLongHigh(value)
                iidLow = iidReader.getLongLongLow(value)
            }
        }

    init {
        // done separately s.t. the setter is called
        index = indexOf(path)
    }

    val maxIndex: Int = Bucketer.DEFAULT.incrementPath(path)?.let { indexOf(it) } ?: relReader.rowCount

    fun nextIndex() = ++index

    fun getIidPointer(reuse: ArrowBufPointer) = iidReader.getPointer(index, reuse)

    val systemFrom get() = sysFromReader.getLong(index)
    val validFrom get() = validFromReader.getLong(index)
    val validTo get() = validToReader.getLong(index)
    val op get() = opReader.getLeg(index)!!

    fun isValid(): Boolean = index < maxIndex

    companion object {
        @JvmStatic
        fun comparator(): Comparator<in EventRowPointer> = Comparator { l, r ->
            compareUnsigned(l.iidHigh, r.iidHigh).takeIf { it != 0 }?.let { return@Comparator it }
            compareUnsigned(l.iidLow, r.iidLow).takeIf { it != 0 }?.let { return@Comparator it }
            r.systemFrom.compareTo(l.systemFrom)
        }
    }
}
