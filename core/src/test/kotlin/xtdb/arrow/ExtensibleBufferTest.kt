package xtdb.arrow

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ReferenceManager
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import java.nio.ByteBuffer

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

    @Test
    fun `writing a ByteBuffer appends its remaining bytes and leaves its position alone`(al: BufferAllocator) {
        val bytes = ByteArray(300) { it.toByte() }

        fun direct() = ByteBuffer.allocateDirect(bytes.size).put(bytes).flip()

        val srcs = mapOf(
            "heap" to ByteBuffer.wrap(bytes),
            "heap, positioned" to ByteBuffer.wrap(bytes).position(7),
            "heap slice" to ByteBuffer.wrap(bytes, 5, 200).slice(),
            "read-only heap" to ByteBuffer.wrap(bytes).asReadOnlyBuffer().position(3),
            "direct" to direct().position(11),
            "read-only direct" to direct().asReadOnlyBuffer().limit(290),
        )

        for ((desc, src) in srcs) {
            ExtensibleBuffer(al).use { buf ->
                buf.writeInt(42)
                val expected = src.duplicate()

                buf.writeBytes(src)
                buf.writeByte(43)

                withClue(desc) {
                    src.position() shouldBe expected.position()
                    buf.getInt(0) shouldBe 42
                    (buf.getBytes(Int.SIZE_BYTES, expected.remaining()) == expected) shouldBe true
                    buf.getBytes(Int.SIZE_BYTES + expected.remaining(), 1).get() shouldBe 43.toByte()
                }
            }
        }
    }
}
