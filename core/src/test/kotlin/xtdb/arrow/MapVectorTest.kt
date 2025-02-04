package xtdb.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.SeekableReadChannel
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.Text
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE as STRUCT_TYPE

class MapVectorTest {
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
    fun mapVector() {
        val m1 = mapOf(4 to "Hello", 8 to null)
        val m2 = mapOf(12 to "world")
        val m3 = mapOf(3 to "!")

        val children = linkedMapOf(
            "key" to IntVector(allocator, "key", false),
            "value" to Utf8Vector(allocator, "value", true)
        )

        val struct = StructVector(allocator, "struct", false, children)
        val list = ListVector(allocator, "entries", false, struct)
        MapVector(list, true).use { mapVec ->
            mapVec.writeObject(m1)
            mapVec.writeObject(m2)
            mapVec.writeObject(m3)

            assertEquals(listOf(m1, m2, m3), mapVec.asList)
        }
    }

    @Test
    fun roundTripsThroughFile() {
        val m1 = mapOf(4 to "Hello", 8 to null)
        val m2 = mapOf(12 to "world")
        val m3 = mapOf(3 to "!")

        val children = linkedMapOf(
            "key" to IntVector(allocator, "key", false),
            "value" to Utf8Vector(allocator, "value", true)
        )

        val struct = StructVector(allocator, "entries", false, children)
        val list = ListVector(allocator, "map", false, struct)
        val map = MapVector(list, true)

        val buf = ByteArrayOutputStream()

        Relation(listOf(map)).use { rel ->
            rel.startUnload(Channels.newChannel(buf)).use { unloader ->
                map.writeObject(m1)
                rel.endRow()

                map.writeObject(m2)
                rel.endRow()

                unloader.writePage()
                rel.clear()

                map.writeObject(m3)
                rel.endRow()

                unloader.writePage()
                unloader.end()
            }
        }

        Relation.loader(allocator, buf.toByteArray().asChannel).use { loader ->
            Relation(allocator, loader.schema).use { rel ->
                val mapVec = rel["map"]!!

                assertEquals(2, loader.pageCount)

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(listOf(m1, m2), mapVec.asList)

                loader.loadPage(1, rel)

                assertEquals(1, rel.rowCount)
                assertEquals(listOf(m3), mapVec.asList)

                loader.loadPage(0, rel)

                assertEquals(2, rel.rowCount)
                assertEquals(listOf(m1, m2), mapVec.asList)
            }
        }

        // reads back in with original Arrow
        val rdr = ArrowFileReader(SeekableReadChannel(buf.toByteArray().asChannel), allocator)
            .also { it.initialize() }

        rdr.vectorSchemaRoot.use { vsr ->
            assertEquals(
                Schema(
                    listOf(
                        Field(
                            "map", FieldType.notNullable(ArrowType.Map(true)),
                            listOf(
                                Field(
                                    "entries", FieldType.notNullable(STRUCT_TYPE),
                                    listOf(
                                        Field("key", FieldType.notNullable(MinorType.INT.type), emptyList()),
                                        Field("value", FieldType.nullable(MinorType.VARCHAR.type), emptyList()),
                                    )
                                )
                            )
                        )
                    )
                ),
                vsr.schema
            )

            val mapVec = vsr.getVector("map")

            assertTrue(rdr.loadNextBatch())
            assertEquals(2, vsr.rowCount)
            assertEquals(listOf(mapOf("key" to 4, "value" to Text("Hello")), mapOf("key" to 8)), mapVec.getObject(0))
            assertEquals(listOf(mapOf("key" to 12, "value" to Text("world"))), mapVec.getObject(1))

            assertTrue(rdr.loadNextBatch())
            assertEquals(1, vsr.rowCount)
            assertEquals(listOf(mapOf("key" to 3, "value" to Text("!"))), mapVec.getObject(0))

            assertFalse(rdr.loadNextBatch())
        }


    }

}
