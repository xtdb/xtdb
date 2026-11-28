package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Incorrect
import xtdb.arrow.IID_TYPE
import xtdb.arrow.Relation
import xtdb.arrow.VectorIndirection.Mask
import xtdb.arrow.VectorWriter
import xtdb.arrow.VectorType.Companion.IID
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
                val iids = rel.vectorFor("_iid", IID_TYPE, false)

                for (uuid in uuids) {
                    iids.writeUuid(uuid)
                    rel.endRow()
                }

                iidSel.select(rel).toSelection()
            }

        assertArrayEquals(IntArray(0), testUuids(), "empty")
        assertArrayEquals(IntArray(0), testUuids(before, before), "only smaller UUIDs")
        assertArrayEquals(IntArray(0), testUuids(after, after), "only larger UUIDs")
        assertArrayEquals(intArrayOf(0, 1), testUuids(search, search), "only matching UUIDs")
        assertArrayEquals(intArrayOf(1, 2), testUuids(before, search, search), "smaller UUIDs and no larger ones")
        assertArrayEquals(intArrayOf(0, 1), testUuids(search, search, after), "no smaller UUIDs but larger ones")
        assertArrayEquals(intArrayOf(1, 2), testUuids(before, search, search, after), "general case")
    }

    @Test
    fun `selects only the matching rows the input mask sets`() {
        val other = Uuid.parse("00000000-0000-0000-0000-000000000000")
        val search = Uuid.parse("80000000-0000-0000-0000-000000000000")

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID_TYPE, false)
            for (uuid in listOf(other, search, search, search)) {
                iids.writeUuid(uuid)
                rel.endRow()
            }

            val mask = Mask.ofWords(longArrayOf(0b1011), 4)
            assertArrayEquals(intArrayOf(1, 3), SingleIidSelector(search.toByteArray()).select(rel, mask).toSelection())
        }
    }

    @Test
    fun `selects exactly the rows of each iid, where iids differ only in their low half or across a sign bit`() {
        val uuids = IID_ORDERING_BOUNDARIES

        Relation(al).use { rel ->
            val iids = rel.vectorFor("_iid", IID_TYPE, false)
            for (uuid in uuids) repeat(2) {
                iids.writeUuid(uuid)
                rel.endRow()
            }

            uuids.forEachIndexed { idx, uuid ->
                assertArrayEquals(intArrayOf(idx * 2, idx * 2 + 1), SingleIidSelector(uuid.toByteArray()).select(rel).toSelection(), "$uuid")
            }
        }
    }

    @Test
    fun `an iid that isn't 16 bytes is rejected`() {
        assertThrows<Incorrect> { SingleIidSelector(ByteArray(15)) }
        assertThrows<Incorrect> { SingleIidSelector(ByteArray(17)) }
    }
}
