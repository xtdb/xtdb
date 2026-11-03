package xtdb.postgres

import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertEquals

class PgOutputMessageTest {

    @Test
    fun `a truncate message lists every relation it names`() {
        val buf = ByteBuffer.allocate(1 + 4 + 1 + 3 * 4)
            .put('T'.code.toByte())
            .putInt(3)
            .put(1) // CASCADE
            .putInt(16384).putInt(16390).putInt(16402)
            .flip()

        assertEquals(PgOutputMessage.Truncate(listOf(16384, 16390, 16402)), PgOutputMessage.parse(buf))
    }
}
