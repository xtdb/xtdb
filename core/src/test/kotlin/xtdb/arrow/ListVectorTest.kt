package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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
        ListVector(allocator, "list", false, NullVector("\$data\$")).use { listVec ->
            listVec.writeObject(listOf(1, 2, 3))
            listVec.writeObject(listOf(4, 5, "6"))
            assertEquals(listOf(listOf(1, 2, 3), listOf(4, 5, "6")), listVec.asList)

            assertEquals(
                Field(
                    "list", FieldType(false, LIST, null),
                    listOf(
                        Field(
                            "\$data\$", FieldType(false, VectorType.UNION_TYPE, null),
                            listOf(
                                Field("i32", FieldType(false, I32, null), null),
                                Field("utf8", FieldType(false, UTF8_TYPE, null), null)
                            )
                        )
                    )
                ),
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