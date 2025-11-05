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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class BitBufferTest {

    private val BitBuffer.asBooleans get() = BooleanArray(writerBitIndex) { getBit(it) }

    internal fun BitBuffer.writeBoolean(bit: Boolean) = writeBit(if (bit) 1 else 0)

    @Test
    fun testBufferedBitWriting(al: BufferAllocator) {
        BitBuffer(al, 64).use { buf ->
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            
            buf.asBooleans shouldBe booleanArrayOf(true, false, true, true, false, false, true, false)
        }
    }

    @Test
    fun testBufferedBitsAcrossMultipleBytes(al: BufferAllocator) {
        BitBuffer(al, 64).use { buf ->
            repeat(20) { i ->
                buf.writeBoolean(i % 2 == 0)
            }
            
            val expected = BooleanArray(20) { i -> i % 2 == 0 }
            buf.asBooleans shouldBe expected
        }
    }

    @Test
    fun testUnloadBufferFlushesBufferedBits(al: BufferAllocator) {
        BitBuffer(al, 64).use { buf ->
            // Write 5 bits (not a full byte, so they'll be buffered)
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            
            // Before unload, bits should be readable via getBit (which checks buffered bits)
            buf.asBooleans shouldBe booleanArrayOf(true, false, true, false, true)
            
            val buffers = mutableListOf<ArrowBuf>()
            buf.unloadBuffer(buffers)
            buffers.size shouldBe 1
            
            // After unload, the buffered bits should have been flushed to the ArrowBuf
            // We can verify this by checking that the ArrowBuf has the correct data
            val arrowBuf = buffers[0]
            val byte0 = arrowBuf.getByte(0).toInt() and 0xFF
            // Bits: true=1, false=0, true=1, false=0, true=1
            // In byte: bit0=1, bit1=0, bit2=1, bit3=0, bit4=1, rest=0
            // Binary: 00010101 = 0x15 = 21
            byte0 shouldBe 0x15
        }
    }

    @Test
    fun testOpenSliceFlushesBufferedBits(al: BufferAllocator) {
        BitBuffer(al, 64).use { buf ->
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            
            buf.openSlice(al).use { slice ->
                slice.asBooleans shouldBe booleanArrayOf(true, false, true, false, true)
            }
        }
    }

    @Test
    fun testClearResetsBufferedBits(al: BufferAllocator) {
        BitBuffer(al, 64).use { buf ->
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            
            buf.clear()
            buf.writerBitIndex shouldBe 0
            
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            buf.asBooleans shouldBe booleanArrayOf(false, true)
        }
    }

    @Test
    fun testWriteBitWithIndexFlushesBufferedBits(al: BufferAllocator) {
        BitBuffer(al, 64).use { buf ->
            buf.writeBoolean(true)
            buf.writeBoolean(false)
            buf.writeBoolean(true)
            
            buf.writeBit(5, 1)
            
            buf.asBooleans shouldBe booleanArrayOf(true, false, true, false, false, true)
        }
    }

    @Test
    fun testUnsafeWriteBitsProps(al: BufferAllocator) = runTest {
        data class TestCase(val srcBits: List<Boolean>, val offset: Int, val length: Int, val destBits: List<Boolean>)

        checkAll(
            arbitrary {
                val srcBits = Arb.list(Arb.boolean(), 0..64).bind()
                val offset = Arb.nonNegativeInt(srcBits.size).bind()
                val len = Arb.nonNegativeInt(srcBits.size - offset).bind()
                val destBits = Arb.list(Arb.boolean(), 0..(64 - srcBits.size)).bind()
                TestCase(srcBits, offset, len, destBits)
            }
        ) { (srcBits, offset, len, destBits) ->
            BitBuffer(al, 64).use { srcBuf ->
                srcBits.forEach { srcBuf.writeBoolean(it) }

                srcBuf.asBooleans shouldBe srcBits

                BitBuffer(al, 64).use { dest ->
                    destBits.forEach { dest.writeBoolean(it) }

                    dest.unsafeWriteBits(srcBuf, offset, len)

                    dest.asBooleans shouldBe (destBits + srcBits.subList(offset, offset + len)).toBooleanArray()
                }
            }
        }
    }
}
