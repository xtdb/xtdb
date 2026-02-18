package xtdb.indexer

import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VectorType.Companion.I64
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.Trie
import xtdb.util.RowCounter
import java.nio.ByteBuffer

class LiveTableTest {

    @Test
    fun `importData appends rows to liveRelation and updates trie`() {
        RootAllocator().use { allocator ->
            val table = TableRef("test", "public", "docs")
            val rowCounter = RowCounter()

            LiveTable(allocator, BufferPool.UNUSED, table, rowCounter).use { liveTable ->
                Trie.openLogDataWriter(allocator).use { sourceRel ->
                    sourceRel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16)))
                    sourceRel["_system_from"].writeLong(1000L)
                    sourceRel["_valid_from"].writeLong(1000L)
                    sourceRel["_valid_to"].writeLong(Long.MAX_VALUE)
                    sourceRel["op"].vectorFor("put", STRUCT_TYPE, false)
                        .vectorFor("foo", I64.arrowType, false)
                        .writeLong(42)
                    sourceRel.endRow()

                    sourceRel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16) { 1 }))
                    sourceRel["_system_from"].writeLong(2000L)
                    sourceRel["_valid_from"].writeLong(2000L)
                    sourceRel["_valid_to"].writeLong(Long.MAX_VALUE)
                    sourceRel["op"].vectorFor("put", STRUCT_TYPE, false)
                        .vectorFor("foo", I64.arrowType, false)
                        .writeLong(99)
                    sourceRel.endRow()

                    liveTable.importData(sourceRel)
                }

                assertEquals(2, liveTable.liveRelation.rowCount)
                assertEquals(2L, rowCounter.blockRowCount)

                liveTable.openSnapshot().use { snap ->
                    assertEquals(2, snap.liveRelation.rowCount)
                    assertTrue(snap.columnType("foo").toString().isNotEmpty())
                }
            }
        }
    }

    @Test
    fun `importData accumulates with existing data`() {
        RootAllocator().use { allocator ->
            val table = TableRef("test", "public", "docs")
            val rowCounter = RowCounter()

            LiveTable(allocator, BufferPool.UNUSED, table, rowCounter).use { liveTable ->
                Trie.openLogDataWriter(allocator).use { rel ->
                    rel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16)))
                    rel["_system_from"].writeLong(1000L)
                    rel["_valid_from"].writeLong(1000L)
                    rel["_valid_to"].writeLong(Long.MAX_VALUE)
                    rel["op"].vectorFor("put", STRUCT_TYPE, false)
                        .vectorFor("x", I64.arrowType, false)
                        .writeLong(1)
                    rel.endRow()
                    liveTable.importData(rel)
                }

                assertEquals(1, liveTable.liveRelation.rowCount)

                Trie.openLogDataWriter(allocator).use { rel ->
                    rel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16) { 2 }))
                    rel["_system_from"].writeLong(3000L)
                    rel["_valid_from"].writeLong(3000L)
                    rel["_valid_to"].writeLong(Long.MAX_VALUE)
                    rel["op"].vectorFor("put", STRUCT_TYPE, false)
                        .vectorFor("x", I64.arrowType, false)
                        .writeLong(2)
                    rel.endRow()
                    liveTable.importData(rel)
                }

                assertEquals(2, liveTable.liveRelation.rowCount)
                assertEquals(2L, rowCounter.blockRowCount)
            }
        }
    }
}
