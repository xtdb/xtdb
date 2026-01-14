package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.VectorType.Companion.listTypeOf
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.arrow.VectorType.Companion.fromLegs

class ListVectorTest {

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
    fun `promotes el-vector`() {
        ListVector(allocator, "list", false, NullVector($$"$data$")).use { listVec ->
            listVec.writeObject(listOf(1, 2, 3))
            listVec.writeObject(listOf(4, 5, "6"))
            assertEquals(listOf(listOf(1, 2, 3), listOf(4, 5, "6")), listVec.asList)

            assertEquals(
                "list" ofType listTypeOf(fromLegs(VectorType.I32, VectorType.UTF8)),
                listVec.field
            )
        }
    }

    @Test
    fun `promotes el-vector with a null already written`() {
        ListVector(allocator, "list", false, NullVector("\$data\$")).use { listVec ->
            listVec.writeObject(listOf(null))
            listVec.writeObject(listOf(1, 2, 3))

            assertEquals(listOf(listOf(null), listOf(1, 2, 3)), listVec.asList)
        }
    }
}