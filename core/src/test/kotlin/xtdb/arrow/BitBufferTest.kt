package xtdb.arrow

import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.nonNegativeInt
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.util.DataSizeRoundingUtil.divideBy8Ceil
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class BitBufferTest {

    private val BitBuffer.asBooleans get() = BooleanArray(writerBitIndex) { getBoolean(it) }

    @Test
    fun testUnsafeWriteBitsProps(al: BufferAllocator) = runTest {
        data class TestCase(val srcBits: List<Boolean>, val offset: Int, val length: Int, val destBits: List<Boolean>)

        checkAll(
            arbitrary {
                val srcBits = Arb.list(Arb.boolean(), 0..128).bind()
                val offset = Arb.nonNegativeInt(srcBits.size).bind()
                val len = Arb.nonNegativeInt(srcBits.size - offset).bind()
                val destBits = Arb.list(Arb.boolean(), 0..(128 - srcBits.size)).bind()
                TestCase(srcBits, offset, len, destBits)
            }
        ) { (srcBits, offset, len, destBits) ->
            BitBuffer(al).use { srcBuf ->
                srcBits.forEach { srcBuf.writeBoolean(it) }

                srcBuf.asBooleans shouldBe srcBits

                BitBuffer(al).use { dest ->
                    destBits.forEach { dest.writeBoolean(it) }

                    dest.ensureWritable(len)
                    dest.unsafeWriteBits(srcBuf, offset, len)

                    dest.asBooleans shouldBe (destBits + srcBits.subList(offset, offset + len)).toBooleanArray()
                }
            }
        }
    }

    @Test
    fun testOpenSlice(al: BufferAllocator) = runTest {
        checkAll(
            arbitrary {
                val srcBits = Arb.list(Arb.boolean(), 0..128).bind()
                val split = Arb.nonNegativeInt(srcBits.size).bind()
                Pair(srcBits, split)
            }
        ) { (srcBits, split) ->
            BitBuffer(al).use { srcBuf ->
                val preSplit = srcBits.subList(0, split)
                preSplit.forEach { srcBuf.writeBoolean(it) }

                srcBuf.asBooleans shouldBe preSplit

                srcBuf.openSlice(al).use { slice ->
                    val postSplit = srcBits.subList(split, srcBits.size)
                    postSplit.forEach { slice.writeBoolean(it) }

                    srcBuf.writerBitIndex shouldBe split
                    slice.writerBitIndex shouldBe srcBits.size
                    slice.asBooleans shouldBe srcBits
                }
            }
        }
    }

    @Test
    fun testUnloadReload(al: BufferAllocator) = runTest {
        checkAll(Arb.list(Arb.boolean(), 0..128)) { srcBits ->
            BitBuffer(al).use { srcBuf ->
                srcBits.forEach { srcBuf.writeBoolean(it) }

                val unloaded = mutableListOf<ArrowBuf>()
                srcBuf.unloadBuffer(unloaded)
                BitBuffer(al).use { destBuf ->
                    val unloadedBuf = unloaded.first()
                    unloadedBuf.writerIndex() shouldBe divideBy8Ceil(srcBits.size)
                    destBuf.loadBuffer(unloadedBuf, srcBits.size)

                    destBuf.asBooleans shouldBe srcBits
                }
            }
        }
    }
}
