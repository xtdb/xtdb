@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import org.junit.jupiter.api.Assertions.assertFalse
import xtdb.log.proto.trieMetadata
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import xtdb.database.PartitionValue
import clojure.lang.Keyword
import kotlinx.coroutines.runBlocking
import io.mockk.every
import io.mockk.mockkConstructor
import io.mockk.unmockkConstructor
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.util.closeAllOnCatch
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.TestPartition
import xtdb.api.TransactionKey
import xtdb.log.proto.TrieDetails
import xtdb.block.proto.block
import xtdb.storage.MemoryStorage
import xtdb.table.TableSlug
import xtdb.api.TableRef
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.VectorType.Companion.I64
import xtdb.api.error.Fault
import xtdb.api.error.Incorrect
import xtdb.trie.Bucketer
import xtdb.trie.Trie
import xtdb.time.InstantUtil.asMicros
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import xtdb.api.tx.OpenTx

class LiveIndexTest {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator

    private val errorCodeKey = Keyword.intern("xtdb.error", "code")
    private fun Incorrect.errorCode() = data.valAt(errorCodeKey) as Keyword?

    @BeforeEach
    fun setUp() {
        nodeBase = openBase(openMeterRegistry = false)
        allocator = nodeBase.allocator.newChildAllocator("test", 0, Long.MAX_VALUE)
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
        nodeBase.close()
    }

    private inner class TestDb(private val dbName: String = "xtdb") : AutoCloseable {
        val bp = MemoryStorage(allocator, epoch = 0)
        private val partition = TestPartition(allocator, bp)

        val trieCatalog get() = partition.trieCatalog
        val liveIndex get() = partition.liveIndex
        val state get() = partition.state

        fun openTx(txId: Long, systemTime: Instant) =
            OpenTx(
                allocator, nodeBase, partition.storage, partition.state, dbName,
                TransactionKey(txId, systemTime), null
            )

        fun commitTx(openTx: OpenTx) {
            openTx.writeTxRow(null, null)
            openTx.sealTables().associateTo(mutableMapOf()) { it.ref to it.liveTable }
                .closeAllOnCatch { state.applyTx(openTx.txKey, it) }
        }

        override fun close() {
            partition.close()
            bp.close()
        }
    }

    private fun OpenTx.put(table: TableRef, id: UUID) {
        this.table(table).apply {
            writeId(id)
            writeValidTimeMicros(0, 0)
            putDocWriter.endStruct()
            endPut()
        }
    }

    @Test
    fun `transaction snapshot shows read-your-writes`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx1 ->
                tx1.put(table, UUID.randomUUID())
                db.commitTx(tx1)
            }

            db.openTx(1, Instant.parse("2020-01-02T00:00:00Z")).use { tx2 ->
                tx2.put(table, UUID.randomUUID())

                db.state.openSnapshot(emptyList(), tx2).use { snap ->
                    val tableSnaps = snap.table(table)
                    assertEquals(1, tableSnaps.size, "the tx's own view subsumes the committed one")
                    assertEquals(listOf(2), tableSnaps.map { it.relation.rowCount })
                }
            }
        }
    }

    @Test
    fun `openQuery binds $ N-named args to the placeholders by position`() {
        TestDb().use { db ->
            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                // openQuery takes ownership of args and frees them when the cursor closes.
                val args = Relation.openFromRows(allocator, listOf(mapOf("\$1" to 10L, "\$2" to 20L)))
                tx.openQuery("SELECT ? AS a, ? AS b", args).use { cursor ->
                    val rows = mutableListOf<Map<*, *>>()
                    cursor.forEachRemaining { rel -> rows.addAll(rel.toMaps(SNAKE_CASE_STRING)) }
                    assertEquals(listOf(mapOf("a" to 10L, "b" to 20L)), rows)
                }
            }
        }
    }

    @Test
    fun `openQuery rejects arg columns whose names aren't the positional convention`() {
        TestDb().use { db ->
            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                // names that look meaningful but would be silently rebound by position are rejected
                Relation.openFromRows(allocator, listOf(mapOf("x" to 10L, "y" to 20L))).use { args ->
                    val e = assertThrows<Incorrect> { tx.openQuery("SELECT ? AS a, ? AS b", args) }
                    assertEquals(Keyword.intern("xtdb", "invalid-param-names"), e.errorCode())
                }
            }
        }
    }

    @Test
    fun `uncommitted tx is not visible in the live index`() {
        TestDb().use { db ->
            val foo = TableRef("public", "foo")
            val bar = TableRef("public", "bar")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx0 ->
                tx0.put(foo, UUID(0, 0))
                db.commitTx(tx0)
            }

            db.openTx(1, Instant.parse("2020-01-02T00:00:00Z")).use { tx1 ->
                tx1.put(bar, UUID(0, 0))
                // not committing — bar must not appear
                val live = db.liveIndex.state
                assertNotNull(live.table(foo))
                assertNull(live.table(bar))
            }
        }
    }

    @Test
    fun `external snapshot does not see uncommitted data`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())

                db.state.openSnapshot(null).use { snap ->
                    assertTrue(snap.table(table).isEmpty())
                }

                db.state.openSnapshot(emptyList(), tx).use { snap ->
                    val snaps = snap.table(table)
                    assertEquals(1, snaps.size)
                    assertEquals(1, snaps.single().relation.rowCount)
                }

                db.commitTx(tx)
            }

            db.state.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap ->
                assertEquals(1, snap.table(table).single().relation.rowCount)
            }
        }
    }

    @Test
    fun `concurrent external snapshot is unaffected by a later commit`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx1 ->
                tx1.put(table, UUID.randomUUID())
                db.commitTx(tx1)
            }

            db.state.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snapBefore ->
                val before = snapBefore.table(table).single()
                assertEquals(1, before.relation.rowCount)

                db.openTx(1, Instant.parse("2020-01-02T00:00:00Z")).use { tx2 ->
                    tx2.put(table, UUID.randomUUID())
                    db.commitTx(tx2)
                }

                assertEquals(1, before.relation.rowCount)

                db.state.openSnapshot(Instant.parse("2020-01-02T00:00:00Z")).use { snapAfter ->
                    assertEquals(2, snapAfter.table(table).single().relation.rowCount)
                }
            }
        }
    }

    @Test
    fun `table tx fetched multiple times returns the same tx`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                val t1 = tx.table(table)
                val t2 = tx.table(table)
                assertSame(t1, t2)

                t1.apply {
                    writeId(UUID.randomUUID())
                    writeValidTimeMicros(0, 0)
                    putDocWriter.endStruct()
                    endPut()
                }

                db.state.openSnapshot(emptyList(), tx).use { snap ->
                    assertEquals(1, snap.table(table).single().relation.rowCount)
                }
            }
        }
    }

    @Test
    fun `multiple puts of the same iid are all recorded`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")
            val iid = UUID.randomUUID()

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                repeat(3) { tx.put(table, iid) }
                db.commitTx(tx)
            }

            db.state.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap ->
                assertEquals(3, snap.table(table).single().relation.rowCount)
            }
        }
    }

    @Test
    fun `an imported transaction's rows are readable and count towards the block`() {
        TestDb().use { db ->
            val table = TableRef("public", "docs")

            fun import(txId: Long, systemTime: Instant, iid: Byte) =
                Trie.openLogDataWriter(allocator).use { rel ->
                    rel["_iid"].writeBytes(ByteBuffer.wrap(ByteArray(16) { iid }))
                    rel["_system_from"].writeLong(systemTime.asMicros)
                    rel["_valid_from"].writeLong(systemTime.asMicros)
                    rel["_valid_to"].writeLong(Long.MAX_VALUE)
                    rel["op"].vectorFor("put", STRUCT_TYPE, false).endStruct()
                    rel.endRow()
                    db.state.commitTx(TransactionKey(txId, systemTime), mapOf(table to rel))
                }

            import(0, Instant.parse("2020-01-01T00:00:00Z"), 1)
            import(1, Instant.parse("2020-01-02T00:00:00Z"), 2)

            assertEquals(2, db.liveIndex.state.blockRowCount)

            db.state.openSnapshot(Instant.parse("2020-01-02T00:00:00Z")).use { snap ->
                assertEquals(2, snap.table(table).sumOf { it.relation.rowCount })
            }
        }
    }

    @Test
    fun `a reader never sees an adopted block's tries without its table metadata and the next block's live tables`() {
        TestDb().use { db ->
            val table = TableRef("public", "docs")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())
                db.commitTx(tx)
            }

            val writtenTrie = runBlocking { db.liveIndex.finishBlock(db.bp, 0L, TableSlug::of) }.getValue(table).writtenTrie!!
            val l0 = TrieDetails.newBuilder()
                .setTableName(table.schemaAndTable)
                .setTrieKey(writtenTrie.trieKey)
                .setDataFileSize(writtenTrie.dataFileSize)
                .setTrieMetadata(writtenTrie.trieMetadata)
                .build()

            val seen = mutableListOf<PartitionValue>()
            runBlocking {
                val collector = launch(Dispatchers.Unconfined) { db.state.value.collect { seen += it } }
                db.state.adoptBlock(block { blockIndex = 0 }, listOf(l0), Instant.parse("2020-01-01T00:00:00Z"))
                collector.cancel()
            }

            val withL0 = seen.filter { it.trieCat.l0MaxBlockIdx(table) == 0L }
            assertTrue(withL0.isNotEmpty(), "the block adoption was observed")
            for (v in withL0) {
                assertAll(
                    { assertEquals(0L, v.tableCat.blockIdx, "the block's table metadata is there") },
                    { assertEquals(1L, v.live.blockIdx, "the live tables are the next block's") },
                    { assertTrue(v.live.tables.isEmpty()) },
                    { assertNull(v.liveSnap, "the live snapshot over the finished block's tables is dropped with them") },
                )
            }
        }
    }

    @Test
    fun `a snapshot held across a block's adoption stays readable until it closes`() {
        TestDb().use { db ->
            val table = TableRef("public", "docs")
            val t0 = Instant.parse("2020-01-01T00:00:00Z")

            db.put(0, t0, table)

            db.state.openSnapshot(t0).use { held ->
                db.state.adoptBlock(block { blockIndex = 0 }, listOf(db.finishBlock0L0(table)), t0)

                db.state.openSnapshot(null).use { after ->
                    assertTrue(after.table(table).isEmpty(), "the next snapshot reads the block from its L0")
                }

                assertEquals(1, held.table(table).single().relation.rowCount)
            }
        }
    }

    @Test
    fun `releasing a live snapshot once more than it was held is a fault`() {
        TestDb().use { db ->
            db.put(0, Instant.parse("2020-01-01T00:00:00Z"), TableRef("public", "docs"))

            val liveSnap = LiveSnap.open(allocator, db.state.value.value.live, emptyList(), null)
            liveSnap.release()

            val fault = assertThrows<Fault> { liveSnap.release() }
            assertEquals(Keyword.intern("xtdb/live-snap-over-released"), fault.data.valAt(errorCodeKey))
        }
    }

    @Test
    fun `adopting a block removes its live tables`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())
                db.commitTx(tx)
            }

            assertNotNull(db.liveIndex.state.table(table))

            runBlocking { db.liveIndex.finishBlock(db.bp, 0L, TableSlug::of) }
            db.state.adoptBlock(block { blockIndex = 0 }, emptyList(), Instant.parse("2020-01-01T00:00:00Z"))

            assertNull(db.liveIndex.state.table(table))
        }
    }

    @Test
    fun `a live table superseded by its L0 still declares its column types`() {
        TestDb().use { db ->
            val table = TableRef("public", "docs")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.table(table).writePut(mapOf("_id" to UUID.randomUUID(), "v" to 1L))
                db.commitTx(tx)
            }

            val writtenTrie = runBlocking { db.liveIndex.finishBlock(db.bp, 0L, TableSlug::of) }.getValue(table).writtenTrie!!

            db.state.addTries(
                table,
                listOf(
                    TrieDetails.newBuilder()
                        .setTableName(table.schemaAndTable)
                        .setTrieKey(writtenTrie.trieKey)
                        .setDataFileSize(writtenTrie.dataFileSize)
                        .setTrieMetadata(writtenTrie.trieMetadata)
                        .build()
                ),
                Instant.parse("2020-01-01T00:00:00Z")
            )

            db.openTx(1, Instant.parse("2020-01-02T00:00:00Z")).use { tx ->
                db.state.openSnapshot(emptyList(), tx).use { snap ->
                    // the two symptoms are independent - assertAll so a regression in either is reported,
                    // rather than the table-level one masking the column-level one
                    assertAll(
                        { assertTrue(table in snap.tableInfo, "the table is still resolvable") },
                        { assertEquals(mapOf("v" to I64), snap.columnTypes(table, listOf("v"))) }
                    )
                }
            }
        }
    }

    // the deep relation/trie immutability is pinned in LiveTableTest; here we pin it at the
    // live-index / openSnapshot layer: a held snapshot's rows survive a subsequent finishBlock.
    @Test
    fun `live-index snapshot is immutable across finishBlock`() {
        TestDb().use { db ->
            val table = TableRef("public", "foo")
            val uuid = UUID.fromString("7fffffff-ffff-ffff-4fff-ffffffffffff")

            db.openTx(0, Instant.parse("2000-01-01T00:00:00Z")).use { tx ->
                tx.put(table, uuid)
                db.commitTx(tx)
            }

            db.state.openSnapshot(Instant.parse("2000-01-01T00:00:00Z")).use { snap ->
                val before = snap.table(table).single().relation.rowCount
                runBlocking { db.liveIndex.finishBlock(db.bp, 0L, TableSlug::of) }
                assertEquals(before, snap.table(table).single().relation.rowCount)
                assertEquals(1, before)
            }
        }
    }

    @Test
    fun `snapshot filters live-table once L0 published`() {
        TestDb().use { db ->
            val table = TableRef("public", "dedup-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())
                db.commitTx(tx)
            }

            db.openTx(1, Instant.parse("2020-01-01T00:00:01Z")).use { observerTx ->
                db.state.openSnapshot(emptyList(), observerTx).use { snap ->
                    assertEquals(1, snap.table(table).size, "live-table is the only source before finishBlock")
                    assertEquals(-1L, snap.trieCatSnap.l0MaxBlockIdx(table), "no L0 published yet")
                }

                for ((t, fb) in runBlocking { db.liveIndex.finishBlock(db.bp, 0L, TableSlug::of) }) {
                    val writtenTrie = fb.writtenTrie ?: continue
                    val trieDetails = TrieDetails.newBuilder()
                        .setTableName(t.schemaAndTable)
                        .setTrieKey(writtenTrie.trieKey)
                        .setDataFileSize(writtenTrie.dataFileSize)
                        .also { it.setTrieMetadata(writtenTrie.trieMetadata) }
                        .build()
                    db.state.addTries(t, listOf(trieDetails), Instant.now())
                }

                db.state.openSnapshot(emptyList(), observerTx).use { snap ->
                    assertTrue(snap.table(table).isEmpty(), "live-table must be filtered — its rows now live in L0_0")
                    assertEquals(0L, snap.trieCatSnap.l0MaxBlockIdx(table), "L0_0 must be visible in the snap's trie-cat")
                }
            }
        }
    }

    // the lazy gate: a commit no longer eagerly rebuilds the shared snapshot — only a read demanding a
    // basis the cache doesn't yet satisfy does.
    @Test
    fun `snapshot not rebuilt until a newer basis is demanded`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())
                db.commitTx(tx)
            }

            db.state.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap0 ->
                // a second commit, with no read in between — must not eagerly rebuild the cached snapshot
                db.openTx(1, Instant.parse("2020-01-02T00:00:00Z")).use { tx ->
                    tx.put(table, UUID.randomUUID())
                    db.commitTx(tx)
                }

                db.state.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { reused ->
                    assertSame(snap0.table(table).single(), reused.table(table).single(), "a basis the cache already satisfies reuses its segments — the commit did not rebuild them")
                }

                db.state.openSnapshot(Instant.parse("2020-01-02T00:00:00Z")).use { rebuilt ->
                    assertNotSame(snap0.table(table).single(), rebuilt.table(table).single(), "a newer basis rebuilds them on demand")
                }
            }
        }
    }

    private fun l1Trie(table: TableRef, trieKey: String): TrieDetails =
        TrieDetails.newBuilder()
            .setTableName(table.schemaAndTable).setTrieKey(trieKey).setDataFileSize(100)
            .setTrieMetadata(trieMetadata {})
            .build()

    private fun TestDb.put(txId: Long, systemTime: Instant, table: TableRef) =
        openTx(txId, systemTime).use { tx ->
            tx.put(table, UUID.randomUUID())
            commitTx(tx)
        }

    private fun TestDb.finishBlock0L0(table: TableRef): TrieDetails {
        val writtenTrie = runBlocking { liveIndex.finishBlock(bp, 0L, TableSlug::of) }.getValue(table).writtenTrie!!
        return TrieDetails.newBuilder()
            .setTableName(table.schemaAndTable)
            .setTrieKey(writtenTrie.trieKey)
            .setDataFileSize(writtenTrie.dataFileSize)
            .setTrieMetadata(writtenTrie.trieMetadata)
            .build()
    }

    @Test
    fun `a compaction's tries reach the next query without a new transaction`() {
        TestDb().use { db ->
            val table = TableRef("public", "docs")
            val t1 = Instant.parse("2020-01-02T00:00:00Z")

            db.put(0, Instant.parse("2020-01-01T00:00:00Z"), table)
            db.state.adoptBlock(block { blockIndex = 0 }, listOf(db.finishBlock0L0(table)), Instant.parse("2020-01-01T00:00:00Z"))
            db.put(1, t1, table)

            db.state.openSnapshot(t1).use { before ->
                db.state.addTries(table, listOf(l1Trie(table, "l01-rc-b00")), t1)

                db.state.openSnapshot(t1).use { after ->
                    assertAll(
                        { assertFalse("l01-rc-b00" in before.trieCatSnap.listAllTrieKeys(table)) },
                        { assertTrue("l01-rc-b00" in after.trieCatSnap.listAllTrieKeys(table)) },
                        { assertEquals(before.txBasis, after.txBasis) },
                        { assertSame(before.table(table).single(), after.table(table).single(), "the live segments are shared, not re-sliced") },
                    )
                }
            }
        }
    }

    @Test
    fun `a snapshot held across tries updates and a rebuild stays readable until it closes`() {
        TestDb().use { db ->
            val table = TableRef("public", "docs")
            val t1 = Instant.parse("2020-01-02T00:00:00Z")
            val t2 = Instant.parse("2020-01-03T00:00:00Z")

            db.put(0, Instant.parse("2020-01-01T00:00:00Z"), table)
            db.state.adoptBlock(block { blockIndex = 0 }, listOf(db.finishBlock0L0(table)), Instant.parse("2020-01-01T00:00:00Z"))
            db.put(1, t1, table)

            db.state.openSnapshot(t1).use { held ->
                db.state.addTries(table, listOf(l1Trie(table, "l01-rc-b00")), t1)
                db.state.deleteTries(table, setOf("l01-rc-b00"))

                db.put(2, t2, table)
                db.state.openSnapshot(t2).use { rebuilt ->
                    assertEquals(2, rebuilt.table(table).single().relation.rowCount)
                }

                assertEquals(1, held.table(table).single().relation.rowCount)
            }
        }
    }

    @Test
    fun `a snapshot held across a block adoption that races a fresher snapshot stays readable and nothing leaks`() {
        val inMetadata = CountDownLatch(1)
        val resumeAdoption = CountDownLatch(1)

        mockkConstructor(LiveIndex.State::class)
        try {
            every { anyConstructed<LiveIndex.State>().blockMetadata() } answers {
                inMetadata.countDown()
                resumeAdoption.await()
                callOriginal()
            }

            TestDb().use { db ->
                val table = TableRef("public", "docs")
                val t0 = Instant.parse("2020-01-01T00:00:00Z")
                val t1 = Instant.parse("2020-01-02T00:00:00Z")

                db.put(0, t0, table)

                db.state.openSnapshot(t0).use { held ->
                    db.put(1, t1, table)
                    val l0 = db.finishBlock0L0(table)

                    var adoption: Throwable? = null
                    val adopting = thread {
                        try {
                            db.state.adoptBlock(block { blockIndex = 0 }, listOf(l0), t1)
                        } catch (e: Throwable) {
                            adoption = e
                        }
                    }

                    try {
                        assertTrue(inMetadata.await(30, TimeUnit.SECONDS), "adoption reached blockMetadata()")
                        db.state.openSnapshot(t1).use { }
                    } finally {
                        resumeAdoption.countDown()
                        adopting.join(30_000)
                        assertFalse(adopting.isAlive, "adoption finished")
                    }

                    assertNull(adoption)
                    assertEquals(1, held.table(table).single().relation.rowCount)
                }
            }
        } finally {
            unmockkConstructor(LiveIndex.State::class)
        }
    }

    @Test
    fun `an L0 reaching the trie catalog ahead of its block's adoption hides the live rows it covers but not their types`() {
        TestDb().use { db ->
            val table = TableRef("public", "docs")
            val t0 = Instant.parse("2020-01-01T00:00:00Z")

            db.openTx(0, t0).use { tx ->
                tx.table(table).writePut(mapOf("_id" to UUID.randomUUID(), "v" to 1L))
                db.commitTx(tx)
            }

            db.state.openSnapshot(t0).use { before ->
                assertEquals(1, before.table(table).size)
            }

            db.state.addTries(table, listOf(db.finishBlock0L0(table)), t0)

            db.state.openSnapshot(t0).use { after ->
                assertAll(
                    { assertEquals(0L, after.trieCatSnap.l0MaxBlockIdx(table)) },
                    { assertTrue(after.table(table).isEmpty(), "the L0 covers the live table's rows") },
                    { assertEquals(mapOf("v" to I64), after.columnTypes(table, listOf("v"))) },
                )
            }
        }
    }

    @Test
    fun `gated snapshot reflects its demanded basis`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())
                db.commitTx(tx)
            }

            db.state.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap ->
                assertEquals(
                    Instant.parse("2020-01-01T00:00:00Z"), snap.txBasis?.systemTime,
                    "snapshot basis covers the demanded tx"
                )
                assertEquals(
                    1, snap.table(table).single().relation.rowCount,
                    "the row committed at the demanded basis is visible"
                )
            }
        }
    }

    @Test
    fun `bucketFor derives the iid path`() {
        val uuid = UUID.fromString("ce33e4b8-ec2f-4b80-8e9c-a4314005adbf")
        val iid = ByteBuffer.allocate(16)
            .putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
        val bucketer = Bucketer.DEFAULT

        assertEquals(
            listOf<Byte>(3, 0, 3, 2, 0, 3, 0, 3, 3, 2, 1, 0, 2, 3, 2, 0),
            (0 until 16).map { bucketer.bucketFor(iid, it) }
        )

        assertEquals(3.toByte(), bucketer.bucketFor(iid, 18))
        assertEquals(0.toByte(), bucketer.bucketFor(iid, 30))
        assertEquals(3.toByte(), bucketer.bucketFor(iid, 63))
    }

    // Ownership across the hand-over to the live index. The fixture's `allocator.close()` is the
    // assertion that matters in both: a table owned by neither side at the end fails the run there,
    // whatever these bodies assert.

    @Test
    fun `apply takes every table out of the map it is handed`() {
        TestDb().use { db ->
            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(TableRef("public", "docs"), UUID.randomUUID())
                tx.writeTxRow(null, null)

                val tables = tx.sealTables().associateTo(mutableMapOf()) { it.ref to it.liveTable }
                assertTrue(tables.isNotEmpty())

                db.state.applyTx(tx.txKey, tables)

                assertTrue(
                    tables.isEmpty(),
                    "drained once taken, so what is left after a throw is exactly what never landed"
                )
            }
        }
    }

    @Test
    fun `a tx whose apply faults frees every table it staged`() {
        TestDb().use { db ->
            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(TableRef("public", "docs"), UUID.randomUUID())
                tx.put(TableRef("public", "more_docs"), UUID.randomUUID())
                tx.writeTxRow(null, null)

                assertThrows<IllegalStateException> {
                    tx.sealTables().associateTo(mutableMapOf()) { it.ref to it.liveTable }
                        .closeAllOnCatch { error("apply faulted") }
                }

                assertNull(db.liveIndex.state.table(TableRef("public", "docs")), "nothing half-installed")
            }
        }
    }
}
