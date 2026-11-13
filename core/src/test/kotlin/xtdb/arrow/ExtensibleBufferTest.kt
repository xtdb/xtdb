package xtdb.arrow

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ReferenceManager
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver

@ExtendWith(AllocatorResolver::class)
class ExtensibleBufferTest {

    @Test
    fun `a buffer whose load fails keeps the buffer it held`(al: BufferAllocator) {
        val refMgr = mockk<ReferenceManager>(relaxed = true)
        every { refMgr.transferOwnership(any(), any()) } throws OutOfMemoryError("Java heap space")

        ExtensibleBuffer(al).use { buf ->
            buf.writeInt(42)

            shouldThrow<OutOfMemoryError> { buf.loadBuffer(ArrowBuf(refMgr, null, 8, 0)) }

            buf.getInt(0) shouldBe 42
        }
    }
}
