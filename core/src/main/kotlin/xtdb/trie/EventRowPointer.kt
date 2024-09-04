package xtdb.trie

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.vector.IVectorReader
import xtdb.vector.RelationReader as OldRelationReader

interface EventRowPointer {
    val index: Int

    val systemFrom: Long
    val validFrom: Long
    val validTo: Long
    val op: String

    fun nextIndex(): Int
    fun getIidPointer(reuse: ArrowBufPointer): ArrowBufPointer
    fun isValid(reuse: ArrowBufPointer, path: ByteArray): Boolean

    class XtArrow(val relReader: RelationReader, path: ByteArray): EventRowPointer {
        private val iidReader: VectorReader = relReader["_iid"]!!

        private val sysFromReader: VectorReader = relReader["_system_from"]!!
        private val validFromReader: VectorReader = relReader["_valid_from"]!!
        private val validToReader: VectorReader = relReader["_valid_to"]!!

        private val opReader: VectorReader = relReader["op"]!!

        override var index: Int private set

        init {
            var left = 0
            var right = relReader.rowCount
            var mid: Int
            while (left < right) {
                mid = (left + right) / 2
                if (HashTrie.compareToPath(iidReader.getPointer(mid), path) < 0) left = mid + 1
                else right = mid
            }
            this.index = left
        }

        override fun nextIndex() = ++index

        override fun getIidPointer(reuse: ArrowBufPointer) = iidReader.getPointer(index, reuse)

        override val systemFrom get() = sysFromReader.getLong(index)
        override val validFrom get() = validFromReader.getLong(index)
        override val validTo get() = validToReader.getLong(index)
        override val op get() = opReader.getLeg(index)!!

        override fun isValid(reuse: ArrowBufPointer, path: ByteArray): Boolean =
            index < relReader.rowCount && HashTrie.compareToPath(getIidPointer(reuse), path) <= 0
    }

    class Arrow(val relReader: OldRelationReader, path: ByteArray): EventRowPointer {
        private val iidReader: IVectorReader = relReader.readerForName("_iid")

        private val sysFromReader: IVectorReader = relReader.readerForName("_system_from")
        private val validFromReader: IVectorReader = relReader.readerForName("_valid_from")
        private val validToReader: IVectorReader = relReader.readerForName("_valid_to")

        private val opReader: IVectorReader = relReader.readerForName("op")

        override var index: Int private set

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

        override fun nextIndex() = ++index

        override fun getIidPointer(reuse: ArrowBufPointer) = iidReader.getPointer(index, reuse)

        override val systemFrom get() = sysFromReader.getLong(index)
        override val validFrom get() = validFromReader.getLong(index)
        override val validTo get() = validToReader.getLong(index)
        override val op get() = opReader.getLeg(index)

        override fun isValid(reuse: ArrowBufPointer, path: ByteArray): Boolean =
            index < relReader.rowCount() && HashTrie.compareToPath(getIidPointer(reuse), path) <= 0
    }

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
