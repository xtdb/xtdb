package xtdb.arrow

import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.VectorType.Companion.BOOL
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class VectorMaskTest {

    @Test
    fun `ALL reports every index set`() {
        (0 until 8).all { VectorMask.ALL.isSet(it) } shouldBe true
    }

    @Test
    fun `open sets exactly the true rows`(al: BufferAllocator) {
        val bits = listOf(true, false, true, true, false)

        Vector.fromList(al, "v", BOOL, bits).use { vec ->
            VectorMask.open(al, vec.valueCount, vec).use { mask ->
                bits.indices.map { mask.isSet(it) } shouldBe bits
            }
        }
    }

    @Test
    fun `open with an incoming mask is the conjunction of the two`(al: BufferAllocator) {
        val incomingBits = listOf(true, false, true, true)
        val bits = listOf(true, true, false, true)

        Vector.fromList(al, "incoming", BOOL, incomingBits).use { incomingVec ->
            VectorMask.open(al, incomingVec.valueCount, incomingVec).use { incomingMask ->
                Vector.fromList(al, "v", BOOL, bits).use { vec ->
                    VectorMask.open(al, vec.valueCount, vec, incomingMask).use { mask ->
                        bits.indices.map { mask.isSet(it) } shouldBe
                            bits.indices.map { incomingBits[it] && bits[it] }
                    }
                }
            }
        }
    }

    @Test
    fun `closing the mask releases its buffer`(al: BufferAllocator) {
        Vector.fromList(al, "v", BOOL, listOf(true, false)).use { vec ->
            VectorMask.open(al, vec.valueCount, vec).close()
        }
    }
}
