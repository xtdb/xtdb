package xtdb.trie

import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.util.iidPrefix

class IidPrefixFilterTest {

    private val iid = Arb.byteArray(Arb.constant(16), Arb.byte())

    private val maxDepth = 16 * Byte.SIZE_BITS / DEFAULT_LEVEL_BITS

    private fun ByteArray.pathTo(depth: Int) = ByteArray(depth) { Bucketer.DEFAULT.bucketFor(this, it) }

    @Test
    fun `every path to an IID matching on the masked bits passes, whatever the mask and depth`() = runTest {
        checkAll(
            5000, iid, Arb.int().filter { it != 0 }, Arb.list(Arb.int(), 0..4), Arb.int(0..maxDepth)
        ) { iid, mask, others, depth ->
            val filter = IidPrefixFilter(mask, (others + iid.iidPrefix()).shuffled().toIntArray())
            assertTrue(filter.test(iid.pathTo(depth)), "mask ${mask.toUInt().toString(16)}, depth $depth")
        }
    }

    @Test
    fun `a path disagreeing with every value on a masked bit it reaches fails`() = runTest {
        checkAll(5000, iid, Arb.int(0 until Int.SIZE_BITS), Arb.int(0..maxDepth)) { iid, bit, depth ->
            val mask = 1 shl bit
            val filter = IidPrefixFilter(mask, intArrayOf(iid.iidPrefix() xor mask))
            val reachesBit = (Int.SIZE_BITS - 1 - bit) / DEFAULT_LEVEL_BITS < depth

            assertEquals(!reachesBit, filter.test(iid.pathTo(depth)), "bit $bit, depth $depth")
        }
    }

    @Test
    fun `a half-masked level is compared on its masked bit alone`() {
        val filter = IidPrefixFilter(0x40000000, intArrayOf(0x40000000))

        assertTrue(filter.test(byteArrayOf(0b01)))
        assertTrue(filter.test(byteArrayOf(0b11)))
        assertFalse(filter.test(byteArrayOf(0b00)))
        assertFalse(filter.test(byteArrayOf(0b10)))
    }
}
