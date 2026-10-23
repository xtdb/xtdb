package xtdb.trie

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import xtdb.api.error.Fault
import xtdb.arrow.Encoding
import xtdb.arrow.NULL_TYPE
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.RunEndWidth
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VectorType.Companion.I64
import xtdb.trie.EventOp.DELETE
import xtdb.trie.EventOp.ERASE
import xtdb.trie.EventOp.PUT
import java.nio.ByteBuffer

class EventRowPointerTest {

    private val putField = Field(
        "put", FieldType.notNullable(STRUCT_TYPE),
        listOf(Field("x", FieldType.notNullable(I64.arrowType), null))
    )

    private val ops = listOf(PUT, DELETE, ERASE, PUT, ERASE, DELETE)

    private fun Relation.writeEvents(ops: List<EventOp>) {
        ops.forEachIndexed { idx, op ->
            this["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16)))
            this["_system_from"].writeLong((ops.size - idx).toLong())
            this["_valid_from"].writeLong(0)
            this["_valid_to"].writeLong(10)

            if (op == PUT) this["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(mapOf("x" to 0L))
            else this["op"][op.name.lowercase()].writeNull()

            endRow()
        }
    }

    private fun opsOf(rel: RelationReader): List<EventOp> {
        val evPtr = EventRowPointer(rel, ByteArray(0))
        return buildList {
            while (evPtr.isValid()) {
                add(evPtr.op)
                evPtr.nextIndex()
            }
        }
    }

    @Test
    fun `each page's ops read back where the same op has a different type id in each`() {
        RootAllocator().use { al ->
            Relation(al, Trie.dataRelSchema(putField)).use { putFirst ->
                Trie.openLogDataWriter(al).use { putLast ->
                    putFirst.writeEvents(ops)
                    putLast.writeEvents(ops)

                    assertEquals(listOf("put", "delete", "erase"), putFirst["op"].legNames!!.toList())
                    assertEquals(listOf("delete", "erase", "put"), putLast["op"].legNames!!.toList())

                    assertEquals(ops, opsOf(putFirst))
                    assertEquals(ops, opsOf(putLast))
                }
            }
        }
    }

    @Test
    fun `ops read back through a selection`() {
        RootAllocator().use { al ->
            Trie.openLogDataWriter(al).use { rel ->
                rel.writeEvents(ops)

                val sel = intArrayOf(0, 2, 3, 5)
                assertEquals(sel.map { ops[it] }, opsOf(rel.select(sel)))
            }
        }
    }

    @Test
    fun `a leg that isn't an op fails only the row that carries it`() {
        RootAllocator().use { al ->
            Trie.openLogDataWriter(al).use { rel ->
                rel.writeEvents(ops)
                rel["op"].vectorFor("bogus", NULL_TYPE, true)

                assertEquals(ops, opsOf(rel))

                rel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16)))
                rel["_system_from"].writeLong(0)
                rel["_valid_from"].writeLong(0)
                rel["_valid_to"].writeLong(10)
                rel["op"].vectorFor("bogus", NULL_TYPE, true).writeNull()
                rel.endRow()

                val ex = assertThrows<Fault> { opsOf(rel) }
                assertEquals("unknown op leg: bogus", ex.message)
            }
        }
    }

    enum class IidLayout { PLAIN, RUNS, SINGLE_ROW_RUNS }

    private val iids = listOf(1, 1, 1, 2, 3, 3, 3, 3, 4)

    private fun openEvents(al: RootAllocator, layout: IidLayout) =
        Relation(
            al,
            Trie.dataRelSchema(putField, if (layout == IidLayout.PLAIN) Encoding.Plain else Encoding.RunEnd(RunEndWidth.I32))
        ).also { rel ->
            iids.forEachIndexed { idx, b ->
                val iidBytes = ByteArray(16) { b.toByte() }
                when (layout) {
                    IidLayout.RUNS -> (rel["_iid"] as RunEndEncodedVector)
                        .appendLongLong(b * 0x0101010101010101L, b * 0x0101010101010101L)

                    else -> rel["_iid"].writeBytes(ByteBuffer.wrap(iidBytes))
                }
                rel["_system_from"].writeLong((iids.size - idx).toLong())
                rel["_valid_from"].writeLong(0)
                rel["_valid_to"].writeLong(10)
                rel["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(mapOf("x" to 0L))
                rel.endRow()
            }
        }

    private fun walk(rel: RelationReader) = buildList {
        val evPtr = EventRowPointer(rel, ByteArray(0))
        while (evPtr.isValid()) {
            add(Triple(evPtr.iidHigh, evPtr.iidLow, evPtr.systemFrom))
            evPtr.nextIndex()
        }
    }

    private fun skipIids(rel: RelationReader) = buildList {
        val evPtr = EventRowPointer(rel, ByteArray(0))
        while (evPtr.isValid()) {
            add(evPtr.index)
            evPtr.skipToNextIid()
        }
    }

    @ParameterizedTest
    @EnumSource(IidLayout::class)
    fun `a page's events and entities read back the same however its iids are laid out`(layout: IidLayout) {
        RootAllocator().use { al ->
            openEvents(al, IidLayout.PLAIN).use { plain ->
                openEvents(al, layout).use { rel ->
                    assertEquals(walk(plain), walk(rel))
                    assertEquals(listOf(0, 3, 4, 8), skipIids(rel))

                    val sel = intArrayOf(1, 2, 3, 5, 7, 8)
                    assertEquals(walk(plain.select(sel)), walk(rel.select(sel)))
                    assertEquals(listOf(0, 2, 3, 5), skipIids(rel.select(sel)))
                }
            }
        }
    }
}
