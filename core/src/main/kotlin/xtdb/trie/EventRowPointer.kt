package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.vector.IVectorReader
import xtdb.vector.RelationReader

class EventRowPointer(val relReader: RelationReader, path: ByteArray) {
    private val iidReader: IVectorReader = relReader.readerForName("xt\$iid")

    private val sysFromReader: IVectorReader = relReader.readerForName("xt\$system_from")
    private val validFromReader: IVectorReader = relReader.readerForName("xt\$valid_from")
    private val validToReader: IVectorReader = relReader.readerForName("xt\$valid_to")

    private val opReader: IVectorReader = relReader.readerForName("op")

    var index: Int private set

    init {
        var left = 0
        var right = relReader.rowCount()
        var mid: Int
        while (left < right) {
            mid = (left + right) / 2
            if (HashTrie.compareToPath(iidReader.getPointer(mid), path) < 0) left = mid + 1
            else right = mid
        }
        this.index = left
    }

    fun nextIndex() = ++index

    fun getIidPointer(reuse: ArrowBufPointer) = iidReader.getPointer(index, reuse)

    val systemFrom get() = sysFromReader.getLong(index)
    val validFrom get() = validFromReader.getLong(index)
    val validTo get() = validToReader.getLong(index)
    val op get() = opReader.getLeg(index)

    fun isValid(reuse: ArrowBufPointer, path: ByteArray): Boolean =
        index < relReader.rowCount() && HashTrie.compareToPath(getIidPointer(reuse), path) <= 0

    companion object {
        @JvmStatic
        fun comparator(): Comparator<in EventRowPointer> {
            val leftCmp = ArrowBufPointer()
            val rightCmp = ArrowBufPointer()

            return Comparator { l, r ->
                val cmp = l.getIidPointer(leftCmp).compareTo(r.getIidPointer(rightCmp))
                if (cmp != 0) cmp else r.systemFrom.compareTo(l.systemFrom)
            }
        }
    }
}
