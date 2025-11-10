package xtdb.arrow

import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.nonNegativeInt
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import xtdb.test.AllocatorResolver
import xtdb.arrow.VectorType.Companion.ofType
import kotlin.random.Random

@ExtendWith(AllocatorResolver::class)
class VariableWidthVectorTest {
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
    fun `from null into string vector - 3726`() {
        NullVector("v1" ).use { nullVector ->
            nullVector.writeNull()
            nullVector.writeNull()
            nullVector.writeNull()

            Utf8Vector(allocator, "v2", true).use { copy ->
                nullVector.rowCopier(copy).copyRange(0, 3)

                assertEquals(3, copy.valueCount)
                assertNull(copy.getObject(1))
            }
        }
    }

    @Test
    fun `copy rows in batch`(al: BufferAllocator) = runTest {
        checkAll(Arb.list(Arb.string(), 0..12), Arb.int()) { strs, seed ->
            val rand = Random(seed)
            val idxs = (0..<strs.size).shuffled(rand).toIntArray()

            Vector.fromList(al, "foo" ofType VectorType.UTF8, strs).use { src ->
                Vector.open(al, src.field).use { dest ->
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
            val strs = Arb.list(Arb.string(), 1..12).bind()
            val offset = Arb.nonNegativeInt(strs.size - 1).bind()
            val len = Arb.nonNegativeInt(strs.size - 1 - offset).bind()
            Triple(strs, offset, len)
        }) { (strs, offset, len) ->
            Vector.fromList(al, "foo" ofType VectorType.UTF8, strs).use { src ->
                Vector.open(al, src.field).use { dest ->
                    val copier = src.rowCopier(dest)
                    copier.copyRange(offset, len)

                    dest.asList shouldBe (offset..<(offset + len)).map { src[it] }
                }
            }
        }
    }
}