package xtdb.vector.extensions

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.types.LIST_ELS_NAME
import xtdb.types.Type
import xtdb.types.Type.Companion.I64
import xtdb.types.Type.Companion.just
import xtdb.types.Type.Companion.ofType
import xtdb.vector.writerFor

class SetVectorTest {

    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    @Test
    fun `get underlying vector field`() {
        "foo".ofType(just(SetType, children = listOfNotNull(LIST_ELS_NAME ofType I64))).createVector(al).use { sv ->
            val setWriter = writerFor(sv)
            setWriter.writeObject(setOf(1L, 2L, 3L))

            assertEquals(Field("foo", FieldType.notNullable(SetType), listOf(Field("\$data\$", FieldType.notNullable(Types.MinorType.BIGINT.type), emptyList()))), sv.field)

            assertEquals(Field("foo", FieldType.notNullable(ArrowType.List.INSTANCE), listOf(Field("\$data\$", FieldType.notNullable(Types.MinorType.BIGINT.type), emptyList()))),
                (sv as SetVector).underlyingVector.field)
        }
    }
}