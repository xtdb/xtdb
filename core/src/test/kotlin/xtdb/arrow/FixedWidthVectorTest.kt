package xtdb.arrow

import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.nonNegativeInt
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorType.Companion.I32
import xtdb.test.AllocatorResolver
import xtdb.arrow.VectorType.Companion.ofType
import kotlin.collections.map
import kotlin.random.Random
import kotlin.use

@ExtendWith(AllocatorResolver::class)
class FixedWidthVectorTest {
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
    fun testIntVector() {
        IntVector.open(allocator,"foo", false).use { vector ->
            vector.writeInt(42)
            vector.writeInt(43)
            vector.writeInt(44)

            assertEquals(3, vector.valueCount)

            assertEquals(42, vector.getInt(0))
            assertEquals(43, vector.getInt(1))
            assertEquals(44, vector.getInt(2))
        }
    }

    @Test
    fun `a narrower vector's wider getX is unsupported, its getAsX converts`(al: BufferAllocator) {
        ShortVector(al, "s", false).use { vec ->
            vec.writeShort(42)
            assertThrows<UnsupportedOperationException> { vec.getInt(0) }
            assertThrows<UnsupportedOperationException> { vec.getLong(0) }
            vec.getAsInt(0) shouldBe 42
            vec.getAsLong(0) shouldBe 42L
        }

        IntVector.open(al, "i", false).use { vec ->
            vec.writeInt(42)
            assertThrows<UnsupportedOperationException> { vec.getLong(0) }
            vec.getAsLong(0) shouldBe 42L
        }

        FloatVector(al, "f", false).use { vec ->
            vec.writeFloat(4.5f)
            assertThrows<UnsupportedOperationException> { vec.getDouble(0) }
            vec.getAsDouble(0) shouldBe 4.5
        }
    }

    @Test
    fun `a vector's setX is supported only for its own storage type`(al: BufferAllocator) {
        IntVector.open(al, "i", false).use { vec ->
            vec.setInt(0, 42)
            vec[1] = 43
            vec.asList shouldBe listOf(42, 43)
            assertThrows<UnsupportedOperationException> { vec.setLong(2, 44L) }
            assertThrows<UnsupportedOperationException> { vec.setFloat(2, 4.5f) }
            assertThrows<UnsupportedOperationException> { vec.setDouble(2, 4.5) }
        }

        LongVector(al, "l", false).use { vec ->
            vec.setLong(0, 42L)
            vec.asList shouldBe listOf(42L)
            assertThrows<UnsupportedOperationException> { vec.setInt(1, 43) }
            assertThrows<UnsupportedOperationException> { vec.setDouble(1, 4.5) }
        }

        FloatVector(al, "f", false).use { vec ->
            vec.setFloat(0, 4.5f)
            vec.asList shouldBe listOf(4.5f)
            assertThrows<UnsupportedOperationException> { vec.setDouble(1, 4.5) }
        }

        DoubleVector(al, "d", false).use { vec ->
            vec.setDouble(0, 4.5)
            vec.asList shouldBe listOf(4.5)
            assertThrows<UnsupportedOperationException> { vec.setFloat(1, 4.5f) }
            assertThrows<UnsupportedOperationException> { vec.setLong(1, 42L) }
        }

        ShortVector(al, "s", false).use { vec ->
            vec.setShort(0, 42)
            vec.asList shouldBe listOf<Short>(42)
            assertThrows<UnsupportedOperationException> { vec.setInt(1, 42) }
        }

        ByteVector(al, "b", false).use { vec ->
            vec.setByte(0, 42)
            vec.asList shouldBe listOf<Byte>(42)
            assertThrows<UnsupportedOperationException> { vec.setShort(1, 42) }
        }
    }

    @Test
    fun `reserving capacity leaves the value count alone, setting past the end extends it with nulls`(al: BufferAllocator) {
        IntVector.open(al, "foo", true).use { vector ->
            vector.ensureCapacity(8)
            vector.valueCount shouldBe 0

            vector.setInt(2, 42)
            vector.asList shouldBe listOf(null, null, 42)

            vector.setNull(4)
            vector.asList shouldBe listOf(null, null, 42, null, null)
        }
    }

    @Test
    fun `from null into Int vector`() {
        NullVector("v1" ).use { nullVector ->
            nullVector.writeNull()
            nullVector.writeNull()
            nullVector.writeNull()

            IntVector.open(allocator, "v2", true).use { copy ->
                val copier = nullVector.rowCopier(copy)
                copier.copyRow(0)
                copier.copyRow(1)
                copier.copyRow(2)

                assertEquals(3, copy.valueCount)
                assertNull(copy.getObject(1))
            }
        }
    }
    
    @Test
    fun `copy rows in batch`(al: BufferAllocator) = runTest {
        checkAll(Arb.list(Arb.int(), 0..12), Arb.int()) { ints, seed ->
            val rand = Random(seed)
            val idxs = (0..<ints.size).shuffled(rand).toIntArray()

            Vector.fromList(al, "foo", I32, ints).use { src ->
                al.openVector(src.field).use { dest ->
                    val copier = src.rowCopier(dest)
                    copier.copyRows(idxs)

                    dest.asList shouldBe idxs.map { src[it] }
                }
            }
        }
    }

    @Test
    fun `copy range`(al: BufferAllocator) = runTest {
        checkAll(arbitrary {
            val ints = Arb.list(Arb.int(), 1..12).bind()
            val offset = Arb.nonNegativeInt(ints.size - 1).bind()
            val len = Arb.nonNegativeInt(ints.size - 1 - offset).bind()
            Triple(ints, offset, len)
        }) { (ints, offset, len) ->
            Vector.fromList(al, "foo", I32, ints).use { src ->
                al.openVector(src.field).use { dest ->
                    val copier = src.rowCopier(dest)
                    copier.copyRange(offset, len)

                    dest.asList shouldBe (offset..<(offset + len)).map { src[it] }
                }
            }
        }
    }

}