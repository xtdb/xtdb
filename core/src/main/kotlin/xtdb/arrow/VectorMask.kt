package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator

interface VectorMask : AutoCloseable {
    fun isSet(idx: Int): Boolean

    override fun close() {}

    companion object {
        @JvmField
        val ALL: VectorMask = AllOnesMask

        /**
         * @param vec must read a non-null boolean at every index below [rowCount] - a null throws rather than reading as unset.
         */
        @JvmStatic
        @JvmOverloads
        fun open(al: BufferAllocator, rowCount: Int, vec: VectorReader, mask: VectorMask = ALL): VectorMask {
            val bitBuffer = BitBuffer(al, rowCount)
            for (idx in 0 until rowCount)
                if (mask.isSet(idx) && vec.getBoolean(idx)) bitBuffer.setBit(idx, 1)

            return BitBufferMask(bitBuffer)
        }

        internal object AllOnesMask : VectorMask {
            override fun isSet(idx: Int) = true
        }

        internal class BitBufferMask(private val bitBuffer: BitBuffer) : VectorMask {
            override fun isSet(idx: Int) = bitBuffer.getBoolean(idx)
            override fun close() = bitBuffer.close()
        }
    }
}
