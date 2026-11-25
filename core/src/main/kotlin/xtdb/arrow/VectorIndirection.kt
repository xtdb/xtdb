package xtdb.arrow

interface VectorIndirection {
    fun valueCount(): Int

    fun getIndex(idx: Int): Int

    operator fun get(idx: Int): Int = getIndex(idx)

    fun select(idxs: IntArray) = IntArray(idxs.size) { getIndex(idxs[it]) }
    fun select(startIdx: Int, len: Int) = IntArray(len) { getIndex(startIdx + it) }

    abstract class Iterator : IntIterator() {
        /**
         * Positions the iterator so that the next [nextInt] returns the index for logical row [idx].
         */
        abstract fun seek(idx: Int)
    }

    operator fun iterator(): Iterator

    companion object {
        @JvmStatic
        fun selection(idxs: IntArray): VectorIndirection = Selection(idxs)

        internal data class Selection(val idxs: IntArray) : VectorIndirection {
            override fun valueCount(): Int = idxs.size

            override fun getIndex(idx: Int): Int = idxs[idx]

            override fun toString(): String = "(Selection {idxs=${this.idxs.contentToString()}})"

            override operator fun iterator(): VectorIndirection.Iterator =
                object : VectorIndirection.Iterator() {
                    private val idxs = this@Selection.idxs
                    private var idx = 0
                    override fun hasNext() = idx < idxs.size
                    override fun nextInt() = idxs[idx++]

                    override fun seek(idx: Int) {
                        this.idx = idx
                    }
                }

            override fun equals(other: Any?) = when {
                this === other -> true
                other !is Selection -> false
                else -> idxs.contentEquals(other.idxs)
            }

            override fun hashCode() = idxs.contentHashCode()
        }
    }
}
