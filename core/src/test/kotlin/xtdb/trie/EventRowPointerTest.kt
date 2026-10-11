package xtdb.trie

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Fault
import xtdb.arrow.NULL_TYPE
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
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
}
