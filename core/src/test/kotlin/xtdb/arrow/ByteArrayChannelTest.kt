package xtdb.arrow

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.random.Random

class ByteArrayChannelTest {

    @Test
    fun `heap and direct writes concatenate in order past the initial capacity`() {
        val rnd = Random(0)
        val chunks = listOf(10, 1000, 1, 5000, 0, 300).map { rnd.nextBytes(it) }

        val ch = ByteArrayChannel()
        chunks.forEachIndexed { idx, chunk ->
            val buf =
                if (idx % 2 == 0) ByteBuffer.wrap(chunk)
                else ByteBuffer.allocateDirect(chunk.size).put(chunk).flip()

            assertEquals(chunk.size, ch.write(buf))
            assertEquals(0, buf.remaining())
        }

        assertArrayEquals(chunks.reduce(ByteArray::plus), ch.toByteArray())
    }

    @Test
    fun `an unwritten channel yields no bytes`() {
        assertArrayEquals(ByteArray(0), ByteArrayChannel().toByteArray())
    }
}
