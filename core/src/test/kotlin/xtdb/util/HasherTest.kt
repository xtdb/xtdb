package xtdb.util

import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.commons.codec.digest.XXHash32
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import java.nio.ByteBuffer

@ExtendWith(AllocatorResolver::class)
class HasherTest {

    private class ApacheCommonsXx(seed: Int = 0) : Hasher() {
        private val hasher = XXHash32(seed)

        override fun hash(value: ByteBuffer): Int {
            val buf = value.duplicate()
            hasher.update(buf)
            val res = hasher.value
            hasher.reset()
            return (res ushr 32).toInt() xor res.toInt()
        }

        override fun hash(value: ByteArray): Int {
            hasher.update(value)
            val res = hasher.value
            hasher.reset()
            return (res ushr 32).toInt() xor res.toInt()
        }
    }
    
    @Test
    fun `test XXHash32 with byte arrays matches original implementation`() = runTest {
        checkAll(1000, Arb.byteArray(Arb.int(0..1000), Arb.byte()), Arb.int()) { bytes, seed ->
            val originalHash = ApacheCommonsXx(seed).hash(bytes)
            val newHash = Hasher.Xx(seed).hash(bytes)

            assertEquals(originalHash, newHash, "Hash mismatch for byte array of length ${bytes.size} with seed $seed")
        }
    }
    
    @Test
    fun `test XXHash32 with ByteBuffer matches original implementation`() = runTest {
        checkAll(1000, Arb.byteArray(Arb.int(0..1000), Arb.byte()), Arb.int()) { bytes, seed ->
            val originalHash = ApacheCommonsXx(seed).hash(ByteBuffer.wrap(bytes))
            val newHash = Hasher.Xx(seed).hash(ByteBuffer.wrap(bytes))

            assertEquals(originalHash, newHash, "Hash mismatch for ByteBuffer of length ${bytes.size} with seed $seed")
        }
    }
    
    @Test
    fun `test XXHash32 with Double matches original implementation`() = runTest {
        checkAll(1000, Arb.double(), Arb.int()) { value, seed ->
            val originalHash = ApacheCommonsXx(seed).hash(value)
            val newHash = Hasher.Xx(seed).hash(value)

            assertEquals(originalHash, newHash, "Hash mismatch for double $value with seed $seed")
        }
    }
    
    @Test
    fun `test XXHash32 with Long matches original implementation`() = runTest {
        checkAll(1000, Arb.long(), Arb.int()) { value, seed ->
            val originalHash = ApacheCommonsXx(seed).hash(value)
            val newHash = Hasher.Xx(seed).hash(value)

            assertEquals(originalHash, newHash, "Hash mismatch for long $value with seed $seed")
        }
    }
    
    @Test
    fun `test XXHash32 with ArrowBuf matches original implementation`(al: BufferAllocator) = runTest {
        checkAll(1000, Arb.byteArray(Arb.int(0..1000), Arb.byte()), Arb.int()) { bytes, seed ->
            al.buffer(bytes.size.toLong()).use { buf ->
                buf.setBytes(0, bytes)

                val originalHash = ApacheCommonsXx(seed).hash(bytes)
                val newHash = Hasher.Xx(seed).hash(buf, 0, bytes.size)

                assertEquals(originalHash, newHash, "Hash mismatch for ArrowBuf of length ${bytes.size} with seed $seed")
            }
        }
    }
}
