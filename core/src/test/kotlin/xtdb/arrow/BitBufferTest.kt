package xtdb.arrow

import io.kotest.core.tuple
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.util.DataSizeRoundingUtil.divideBy8Ceil
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.VectorIndirection.Companion.Selection
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class BitBufferTest {

    private val BitBuffer.asBooleans get() = BooleanArray(writerBitIndex) { getBoolean(it) }

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

    @Test
    fun testWriteBitsProps(al: BufferAllocator) = runTest {
        checkAll(
            arbitrary {
                val srcBits = Arb.list(Arb.boolean(), 1..128).bind()
                val selection = Arb.intArray(Arb.int(0..64), Arb.nonNegativeInt(srcBits.size - 1)).bind()
                val destBits = Arb.list(Arb.boolean(), 0..16).bind()
                tuple(srcBits, selection, destBits)
            }
        ) { (srcBits, selection, destBits) ->
            BitBuffer(al).use { srcBuf ->
                srcBits.forEach { srcBuf.writeBoolean(it) }

                BitBuffer(al).use { dest ->
                    destBits.forEach { dest.writeBoolean(it) }
                    dest.writeBits(srcBuf, Selection(selection))

                    BitBuffer(al).use { expected ->
                        destBits.forEach { expected.writeBoolean(it) }
                        selection.forEach { idx -> expected.writeBoolean(srcBits[idx]) }

                        dest.asBooleans shouldBe expected.asBooleans
                    }
                }
            }
        }
    }

    @Test
    fun testWriteOnes(al: BufferAllocator) = runTest {
        checkAll(Arb.list(Arb.boolean(), 0..32), Arb.nonNegativeInt(128)) { prefixBits, onesCount ->
            BitBuffer(al).use { buf ->
                prefixBits.forEach { buf.writeBoolean(it) }
                buf.writeOnes(onesCount)

                val actual = buf.asBooleans
                actual.take(prefixBits.size) shouldBe prefixBits
                actual.drop(prefixBits.size).should { l -> l.all { it } }
            }
        }
    }
}
