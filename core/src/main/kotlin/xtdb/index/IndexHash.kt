package xtdb.index

import org.apache.arrow.memory.ArrowBuf
import xtdb.arrow.VectorReader
import xtdb.util.Hasher
import java.nio.ByteBuffer

/**
 * A value's 64-bit index hash: two independently seeded [Hasher.Xx] hashes over the normalisation [VectorReader.hashCode] applies,
 * so values that `=` holds equal hash equal across a union's legs.
 *
 * `-0.0` hashes as `0.0`, which `=` holds equal to it and the 32-bit hash does not.
 */
object IndexHash {
    private class ZeroFolding(seed: Int) : Hasher() {
        private val xx = Xx(seed)

        override fun hash(value: Double) = xx.hash(if (value == 0.0) 0.0 else value)
        override fun hash(value: Long) = xx.hash(value)
        override fun hash(value: ByteArray) = xx.hash(value)
        override fun hash(value: ByteBuffer) = xx.hash(value)
        override fun hash(buf: ArrowBuf, offset: Int, length: Int) = xx.hash(buf, offset, length)
    }

    private val high = ZeroFolding(0)
    private val low = ZeroFolding(0x5bd1e995)

    @JvmStatic
    fun of(rdr: VectorReader, idx: Int): Long =
        (rdr.hashCode(idx, high).toLong() shl 32) or (rdr.hashCode(idx, low).toLong() and 0xffffffffL)
}
