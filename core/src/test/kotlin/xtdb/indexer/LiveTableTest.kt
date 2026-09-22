package xtdb.indexer

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VectorType.Companion.I64
import xtdb.indexer.LiveTable.Companion.logRelTypes
import xtdb.storage.MemoryStorage
import xtdb.api.TableRef
import xtdb.table.TableSlug
import xtdb.trie.MemoryHashTrie
import xtdb.trie.Trie
import xtdb.util.RowCounter
import java.nio.ByteBuffer
import java.util.UUID

class LiveTableTest {

    private val DOCS = TableRef("public", "docs")

    @Test
    fun `importData appends rows to the relation and updates the trie`() {
        RootAllocator().use { allocator ->
            val table = TableRef("public", "docs")
            val rowCounter = RowCounter()

            LiveTable.open(allocator, table, TableSlug.of(table), 0L, rowCounter).use { base ->
                val liveTable = Trie.openLogDataWriter(allocator).use { sourceRel ->
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

                    base.importData(sourceRel)
                }

                assertEquals(2, liveTable.relation.rowCount)
                assertEquals(2L, rowCounter.blockRowCount)

                TableSnapshot.open(allocator, liveTable).use { snap ->
                    assertEquals(2, snap.relation.rowCount)
                    assertTrue(snap.contributedType("foo").toString().isNotEmpty())
                }
            }
        }
    }

    @Test
    fun `importData accumulates with existing data`() {
        RootAllocator().use { allocator ->
            val table = TableRef("public", "docs")
            val rowCounter = RowCounter()

            LiveTable.open(allocator, table, TableSlug.of(table), 0L, rowCounter).use { base ->
                val afterFirst = Trie.openLogDataWriter(allocator).use { rel ->
                    rel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16)))
                    rel["_system_from"].writeLong(1000L)
                    rel["_valid_from"].writeLong(1000L)
                    rel["_valid_to"].writeLong(Long.MAX_VALUE)
                    rel["op"].vectorFor("put", STRUCT_TYPE, false)
                        .vectorFor("x", I64.arrowType, false)
                        .writeLong(1)
                    rel.endRow()
                    base.importData(rel)
                }

                assertEquals(1, afterFirst.relation.rowCount)

                val afterSecond = Trie.openLogDataWriter(allocator).use { rel ->
                    rel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16) { 2 }))
                    rel["_system_from"].writeLong(3000L)
                    rel["_valid_from"].writeLong(3000L)
                    rel["_valid_to"].writeLong(Long.MAX_VALUE)
                    rel["op"].vectorFor("put", STRUCT_TYPE, false)
                        .vectorFor("x", I64.arrowType, false)
                        .writeLong(2)
                    rel.endRow()
                    afterFirst.importData(rel)
                }

                assertEquals(2, afterSecond.relation.rowCount)
                assertEquals(2L, rowCounter.blockRowCount)
            }
        }
    }

    private fun UUID.toIidBytes(): ByteArray =
        ByteBuffer.allocate(16).putLong(mostSignificantBits).putLong(leastSignificantBits).array()

    // a leaf path is a byte per trie level, each holding 2 bits of the iid; four levels reconstruct one iid byte.
    private fun ByteArray.pathToIid(): ByteArray =
        toList().chunked(4)
            .map { (a, b, c, d) -> ((a.toInt() shl 6) or (b.toInt() shl 4) or (c.toInt() shl 2) or d.toInt()).toByte() }
            .toByteArray()

    private fun writePut(rel: xtdb.arrow.Relation, iid: ByteArray, systemFrom: Long, validFrom: Long, validTo: Long) {
        rel["_iid"].writeBytes(iid)
        rel["_system_from"].writeLong(systemFrom)
        rel["_valid_from"].writeLong(validFrom)
        rel["_valid_to"].writeLong(validTo)
        rel["op"].vectorFor("put", STRUCT_TYPE, false).endStruct()
        rel.endRow()
    }

    // every row shares one iid, so the trie can't split: it must bottom out in a single
    // max-depth leaf holding every row index rather than over-splitting or erroring.
    private fun assertCollapsesToMaxDepthLeaf(n: Int, liveTrieFactory: LiveTable.LiveTrieFactory) {
        val uuid = UUID.fromString("7fffffff-ffff-ffff-4fff-ffffffffffff")

        RootAllocator().use { allocator ->
            LiveTable.open(allocator, DOCS, TableSlug.of(DOCS), 0L, RowCounter(), liveTrieFactory).use { base ->
                val liveTable = Trie.openLogDataWriter(allocator).use { sourceRel ->
                    val iid = uuid.toIidBytes()
                    repeat(n) { writePut(sourceRel, iid, 0, 0, 0) }
                    base.importData(sourceRel)
                }

                val leaves = liveTable.trie.compactLogs().leaves
                assertEquals(1, leaves.size)
                assertArrayEquals(uuid.toIidBytes(), leaves.single().path.pathToIid())
                assertEquals((n - 1 downTo 0).toList(), leaves.single().data.toList())
            }
        }
    }

    @Test
    fun `identical iids collapse to a single max-depth leaf`() =
        assertCollapsesToMaxDepthLeaf(1000) { MemoryHashTrie.builder(it).setLogLimit(2).setPageLimit(4).build() }

    // the rigged limits above bottom the trie out in a handful of rows; the production 64/1024
    // want rather more, and it's those we'd regress on.
    @Test
    fun `identical iids collapse to a single max-depth leaf at default trie limits`() =
        assertCollapsesToMaxDepthLeaf(50_000) { MemoryHashTrie.emptyTrie(it) }

    // the relation/trie an already-open snapshot exposes must survive a subsequent finishBlock -
    // TableSnapshot.open takes its own direct slice rather than sharing the live table's mutable state.
    private fun TableSnapshot.snapData(): Pair<List<Map<*, *>>, List<UUID>> {
        // _iid surfaces as a raw ByteArray (compares by reference); normalise so two reads of an
        // unchanged relation are structurally equal.
        val rows = relation.asMaps.map { row -> row.mapValues { (_, v) -> if (v is ByteArray) v.toList() else v } }

        val trie = trie.compactLogs()
        val iidReader = trie.iidReader
        val iids = trie.leaves
            .flatMap { it.data.toList() }
            .map { idx -> iidReader.getBytes(idx).let { UUID(it.getLong(0), it.getLong(8)) } }

        return rows to iids
    }

    @Test
    fun `table snapshot is immutable across finishBlock`() {
        val uuid = UUID.fromString("7fffffff-ffff-ffff-4fff-ffffffffffff")

        RootAllocator().use { allocator ->
            MemoryStorage(allocator, epoch = 0).use { bp ->
                LiveTable.open(allocator, DOCS, TableSlug.of(DOCS), 0L, RowCounter()).use { base ->

                    val liveTable = Trie.openLogDataWriter(allocator).use { sourceRel ->
                        writePut(sourceRel, uuid.toIidBytes(), 0, 0, 0)
                        base.importData(sourceRel)
                    }

                    TableSnapshot.open(allocator, liveTable).use { snap ->
                        val before = snap.snapData()

                        runBlocking { liveTable.finishBlock(bp, 0L) }

                        val after = snap.snapData()

                        assertEquals(before, after)
                        assertEquals(listOf(uuid), after.second)
                    }
                }
            }
        }
    }

    private fun iid(b: Int) = ByteArray(16) { b.toByte() }

    private fun ByteArray.asUuid(): UUID =
        ByteBuffer.wrap(this).let { UUID(it.getLong(0), it.getLong(8)) }

    private fun LiveTable.Tx.writePut(iid: ByteArray, foo: Long) {
        iidVec.writeBytes(iid)
        validFromVec.writeLong(0)
        validToVec.writeLong(Long.MAX_VALUE)
        opVec.vectorFor("put", STRUCT_TYPE, false).vectorFor("foo", I64.arrowType, false).writeLong(foo)
        endOps(1)
    }

    private fun LiveTable.seededWith(allocator: RootAllocator, vararg iids: ByteArray) =
        Trie.openLogDataWriter(allocator).use { rel ->
            for (iid in iids) writePut(rel, iid, 0, 0, 0)
            importData(rel)
        }

    @Test
    fun `a transaction's rows reach the table only when it commits`() {
        RootAllocator().use { allocator ->
            LiveTable.open(allocator, DOCS, TableSlug.of(DOCS), 0L, RowCounter()).use { opened ->
                val base = opened.seededWith(allocator, iid(0))

                val before = TableSnapshot.open(allocator, base).use { it.snapData() }

                val tx = base.openTx(1000L)
                tx.writePut(iid(1), 7)

                assertEquals(2, tx.relation.rowCount, "the transient holds the table's row and its own")
                assertEquals(1, base.relation.rowCount)
                assertEquals(
                    before, TableSnapshot.open(allocator, base).use { it.snapData() },
                    "the table it was opened from is unchanged"
                )

                tx.commit().use { committed ->
                    assertEquals(2, committed.relation.rowCount)
                    assertEquals(
                        listOf(iid(0).asUuid(), iid(1).asUuid()).toSet(),
                        TableSnapshot.open(allocator, committed).use { it.snapData() }.second.toSet()
                    )
                }
            }
        }
    }

    @Test
    fun `a discarded transaction's rows are overwritten by the next one`() {
        RootAllocator().use { allocator ->
            LiveTable.open(allocator, DOCS, TableSlug.of(DOCS), 0L, RowCounter()).use { opened ->
                val base = opened.seededWith(allocator, iid(0))

                base.openTx(1000L).use { discarded ->
                    discarded.writePut(iid(8), 98)
                    discarded.writePut(iid(9), 99)
                }

                assertEquals(1, base.relation.rowCount, "the discarded rows were never the table's")

                val next = base.openTx(2000L)
                next.writePut(iid(1), 7)

                next.commit().use { committed ->
                    assertEquals(2, committed.relation.rowCount)
                    assertEquals(
                        listOf(iid(0).asUuid(), iid(1).asUuid()).toSet(),
                        TableSnapshot.open(allocator, committed).use { it.snapData() }.second.toSet(),
                        "the discarded iids are gone from the trie as well as the relation"
                    )
                }
            }
        }
    }

    @Test
    fun `a discarded transaction's columns never reach the table`() {
        RootAllocator().use { allocator ->
            LiveTable.open(allocator, DOCS, TableSlug.of(DOCS), 0L, RowCounter()).use { opened ->
                val base = opened.seededWith(allocator, iid(0))

                val before = base.relation.logRelTypes

                base.openTx(1000L).use { discarded -> discarded.writePut(iid(9), 99) }

                assertEquals(before, base.relation.logRelTypes)
                assertFalse("foo" in base.relation.logRelTypes.orEmpty())
            }
        }
    }
}
