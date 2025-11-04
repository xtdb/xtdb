package xtdb.arrow

interface VectorIndirection {
    fun valueCount(): Int

    fun getIndex(idx: Int): Int

    operator fun get(idx: Int): Int = getIndex(idx)

    fun select(idxs: IntArray) = IntArray(idxs.size) { getIndex(idxs[it]) }
    fun select(startIdx: Int, len: Int) = IntArray(len) { getIndex(startIdx + it) }

    companion object {
        @JvmStatic
        fun selection(idxs: IntArray): VectorIndirection = Selection(idxs)

        internal data class Selection(val idxs: IntArray) : VectorIndirection {
            override fun valueCount(): Int = idxs.size

            override fun getIndex(idx: Int): Int = idxs[idx]

            override fun toString(): String = "(Selection {idxs=${this.idxs.contentToString()}})"

            override fun equals(other: Any?) = when {
                this === other -> true
                other !is Selection -> false
                else -> idxs.contentEquals(other.idxs)
            }

            override fun hashCode() = idxs.contentHashCode()
        }

        @JvmStatic
        fun slice(startIdx: Int, len: Int): VectorIndirection = Slice(startIdx, len)

        internal data class Slice(val startIdx: Int, val len: Int) : VectorIndirection {
            override fun valueCount(): Int = len
            override fun getIndex(idx: Int): Int = startIdx + idx
        }
    }
}
