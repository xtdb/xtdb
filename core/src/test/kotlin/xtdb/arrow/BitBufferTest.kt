package xtdb.arrow

import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.nonNegativeInt
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class BitBufferTest {

    private val BitBuffer.asBooleans get() = BooleanArray(writerBitIndex) { getBit(it) }

    internal fun BitBuffer.writeBoolean(bit: Boolean) = writeBit(if (bit) 1 else 0)

    @Test
    fun testUnsafeCopyFromProps(al: BufferAllocator) = runTest {
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

                    dest.unsafeCopyFrom(srcBuf, offset, len)

                    dest.asBooleans shouldBe (destBits + srcBits.subList(offset, offset + len)).toBooleanArray()
                }
            }
        }
    }
}
