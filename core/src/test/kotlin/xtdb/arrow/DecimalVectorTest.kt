package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal

class DecimalVectorTest {
    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    @Test
    fun `test DecimalVector`() {
        DecimalVector(
            allocator, "dec",
            true, ArrowType.Decimal(5, 2, 32)
        ).use { dec ->

            dec.writeObject(BigDecimal("123.45"))
            dec.writeObject(BigDecimal("789.01"))
            dec.writeNull()

            assertEquals(3, dec.valueCount)
            assertEquals(listOf(BigDecimal("123.45"), BigDecimal("789.01"), null), dec.toList())


            // precision error
            assertThrows<InvalidWriteObjectException> {
                dec.writeObject(BigDecimal("1234567890.01"))
            }

            // scale error
            assertThrows<InvalidWriteObjectException> {
                dec.writeObject(BigDecimal("12.345"))
            }
        }
    }

    @Test
    fun `test different precisions`() {
        DecimalVector(
            allocator, "dec",
            true, ArrowType.Decimal(32, 2, 128)
        ).use { dec ->

            dec.writeObject(BigDecimal("0.01"))
            dec.writeObject(BigDecimal("24580955505371094.01"))

            assertEquals(2, dec.valueCount)
            assertEquals(listOf(BigDecimal("0.01"), BigDecimal("24580955505371094.01")), dec.toList())
        }
    }
}