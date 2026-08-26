package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator

interface VectorMask : AutoCloseable {
    fun isSet(idx: Int): Boolean

    override fun close() {}

    interface Builder : AutoCloseable {
        fun set(idx: Int)

        /** transfers the underlying buffer to the returned mask, which the caller then owns */
        fun build(): VectorMask
    }

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

        @JvmStatic
        fun openBuilder(al: BufferAllocator, rowCount: Int): Builder = BuilderImpl(BitBuffer(al, rowCount))

        internal object AllOnesMask : VectorMask {
            override fun isSet(idx: Int) = true
        }

        internal class BitBufferMask(private val bitBuffer: BitBuffer) : VectorMask {
            override fun isSet(idx: Int) = bitBuffer.getBoolean(idx)
            override fun close() = bitBuffer.close()
        }

        internal class BuilderImpl(private val bitBuffer: BitBuffer) : Builder {
            private var built = false

            override fun set(idx: Int) {
                bitBuffer.setBit(idx, 1)
            }

            override fun build(): VectorMask = BitBufferMask(bitBuffer).also { built = true }

            override fun close() {
                if (!built) bitBuffer.close()
            }
        }
    }
}
