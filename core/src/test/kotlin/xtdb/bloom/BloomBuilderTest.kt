package xtdb.bloom

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.FixedSizeBinaryVector
import xtdb.arrow.metadata.MetadataFlavour

class BloomBuilderTest {

    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    private fun iid(b: Int) = ByteArray(16) { b.toByte() }

    @Test
    fun `a column with repeated neighbours blooms as if every row were added`() {
        FixedSizeBinaryVector(al, "_iid", false, 16).use { vec ->
            listOf(1, 1, 1, 2, 3, 3, 1, 1).forEach { vec.writeObject(iid(it)) }
            val col: MetadataFlavour.Bytes = vec

            val everyRow = BloomBuilder().also { b -> repeat(col.valueCount) { b.add(col, it) } }.build()

            assertEquals(everyRow, BloomBuilder().also { it.add(col) }.build())
        }
    }
}
