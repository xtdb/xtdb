package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorWriter
import xtdb.types.Type.Companion.IID
import java.nio.ByteBuffer
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
class SingleIidSelectorTest {
    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    private fun VectorWriter.writeUuid(uuid: Uuid) {
        writeBytes(ByteBuffer.wrap(uuid.toByteArray()))
    }

    @Test
    fun testIidSelector() {
        val before = Uuid.parse("00000000-0000-0000-0000-000000000000")
        val search = Uuid.parse("80000000-0000-0000-0000-000000000000")
        val after = Uuid.parse("f0000000-0000-0000-0000-000000000000")

        val iidSel = SingleIidSelector(search.toByteArray())

        fun testUuids(vararg uuids: Uuid) =
            Relation(al).use { rel ->
                val iids = rel.vectorFor("_iid", IID.fieldType)

                for (uuid in uuids) {
                    iids.writeUuid(uuid)
                    rel.endRow()
                }

                iidSel.select(al, rel, emptyMap(), RelationReader.from(emptyList(), 0))
            }

        assertArrayEquals(IntArray(0), testUuids(), "empty")
        assertArrayEquals(IntArray(0), testUuids(before, before), "only smaller UUIDs")
        assertArrayEquals(IntArray(0), testUuids(after, after), "only larger UUIDs")
        assertArrayEquals(intArrayOf(0, 1), testUuids(search, search), "only matching UUIDs")
        assertArrayEquals(intArrayOf(1, 2), testUuids(before, search, search), "smaller UUIDs and no larger ones")
        assertArrayEquals(intArrayOf(0, 1), testUuids(search, search, after), "no smaller UUIDs but larger ones")
        assertArrayEquals(intArrayOf(1, 2), testUuids(before, search, search, after), "general case")
    }

}