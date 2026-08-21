package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.LongLongVectorReader
import xtdb.arrow.RelationReader
import java.lang.Long.compareUnsigned
import kotlin.math.min

class EventRowPointer(private val relReader: RelationReader, path: ByteArray) {
    private val iidReader = relReader["_iid"] as LongLongVectorReader

    private val sysFromReader = relReader["_system_from"]
    private val validFromReader = relReader["_valid_from"]
    private val validToReader = relReader["_valid_to"]

    private val opReader = relReader["op"]

    var iidHigh: Long = -1
        private set

    var iidLow: Long = -1
        private set

    var systemFrom: Long = -1
        private set

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
                systemFrom = sysFromReader.getLong(value)
            }
        }

    init {
        // done separately s.t. the setter is called
        index = indexOf(path)
    }

    val maxIndex: Int = Bucketer.DEFAULT.incrementPath(path)?.let { indexOf(it) } ?: relReader.rowCount

    fun nextIndex() = ++index

    private fun sameIidAt(idx: Int, iidHigh: Long, iidLow: Long) =
        iidReader.getLongLongHigh(idx) == iidHigh && iidReader.getLongLongLow(idx) == iidLow

    /**
     * Advances past every remaining row for the iid this pointer is on.
     *
     * Gallops before bisecting because an entity's run is short relative to the page: doubling finds
     * the end of the run in log(run) reads, where bisecting `[index, maxIndex)` would take log(page).
     */
    fun skipToNextIid() {
        val runIidHigh = iidHigh
        val runIidLow = iidLow

        var onRun = index
        var step = 1
        while (onRun + step < maxIndex && sameIidAt(onRun + step, runIidHigh, runIidLow)) {
            onRun += step
            step *= 2
        }

        var pastRun = min(onRun + step, maxIndex)
        while (pastRun - onRun > 1) {
            val mid = (onRun + pastRun) ushr 1
            if (sameIidAt(mid, runIidHigh, runIidLow)) onRun = mid else pastRun = mid
        }

        index = pastRun
    }

    fun getIidPointer(reuse: ArrowBufPointer) = iidReader.getPointer(index, reuse)

    val validFrom get() = validFromReader.getLong(index)
    val validTo get() = validToReader.getLong(index)
    val op get() = opReader.getLeg(index)!!

    fun isValid(): Boolean = index < maxIndex

    fun sameIidAs(iidHigh: Long, iidLow: Long) = this.iidHigh == iidHigh && this.iidLow == iidLow

    companion object {
        @JvmStatic
        fun comparator(): Comparator<in EventRowPointer> = Comparator { l, r ->
            compareUnsigned(l.iidHigh, r.iidHigh).takeIf { it != 0 }?.let { return@Comparator it }
            compareUnsigned(l.iidLow, r.iidLow).takeIf { it != 0 }?.let { return@Comparator it }
            r.systemFrom.compareTo(l.systemFrom)
        }

        /** Orders pages by the iid each is currently sitting on. */
        @JvmStatic
        fun iidComparator(): Comparator<in EventRowPointer> = Comparator { l, r ->
            compareUnsigned(l.iidHigh, r.iidHigh).takeIf { it != 0 } ?: compareUnsigned(l.iidLow, r.iidLow)
        }

        /** Orders one entity's events, newest system-time first. */
        @JvmStatic
        fun systemFromComparator(): Comparator<in EventRowPointer> =
            Comparator { l, r -> r.systemFrom.compareTo(l.systemFrom) }
    }
}
