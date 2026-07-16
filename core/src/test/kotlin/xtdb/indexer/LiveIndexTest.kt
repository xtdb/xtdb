@file:OptIn(xtdb.InternalApi::class)

package xtdb.indexer

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.NodeBase
import xtdb.NodeBase.Companion.openBase
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.TransactionKey
import xtdb.api.log.InMemoryLog
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.log.proto.TrieDetails
import xtdb.storage.MemoryStorage
import xtdb.api.TableRef
import xtdb.trie.Bucketer
import java.nio.ByteBuffer
import java.time.Instant
import java.time.InstantSource
import java.util.UUID
import xtdb.api.tx.OpenTx

class LiveIndexTest {

    private lateinit var nodeBase: NodeBase
    private lateinit var allocator: BufferAllocator

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

    private inner class TestDb(dbName: String = "xtdb") : AutoCloseable {
        val bp = MemoryStorage(allocator, epoch = 0)
        private val blockCatalog = BlockCatalog(dbName, null)
        private val tableCatalog = TableCatalog(bp)
        val trieCatalog = createTrieCatalog()
        val liveIndex = LiveIndex.open(allocator, blockCatalog, tableCatalog, trieCatalog, dbName)
        private val dbState = DatabaseState(dbName, blockCatalog, tableCatalog, trieCatalog, liveIndex)
        private val dbStorage = DatabaseStorage(
            InMemoryLog<SourceMessage>(InstantSource.system(), 0),
            InMemoryLog<ReplicaMessage>(InstantSource.system(), 0),
            bp, null
        )

        fun openTx(txId: Long, systemTime: Instant) =
            OpenTx(allocator, nodeBase, dbStorage, dbState, TransactionKey(txId, systemTime), null)

        fun commitTx(openTx: OpenTx) {
            openTx.writeTxRow(null, null)
            liveIndex.commitTx(openTx.txKey, openTx.tables.associate { (ref, t) -> ref to t.txRelation })
        }

        override fun close() {
            dbState.close()
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

                // the tx snapshot sees both the committed live table and its own uncommitted write,
                // as two separate entries: live (committed) + tx (uncommitted).
                db.liveIndex.openSnapshot(emptyList(), tx2).use { snap ->
                    val tableSnaps = snap.table(table)
                    assertEquals(2, tableSnaps.size)
                    assertEquals(listOf(1, 1), tableSnaps.map { it.relation.rowCount })
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
                assertNotNull(db.liveIndex.table(foo))
                assertNull(db.liveIndex.table(bar))
            }
        }
    }

    @Test
    fun `external snapshot does not see uncommitted data`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())

                db.liveIndex.openSnapshot(null).use { snap ->
                    assertTrue(snap.table(table).isEmpty())
                }

                db.liveIndex.openSnapshot(emptyList(), tx).use { snap ->
                    val snaps = snap.table(table)
                    assertEquals(1, snaps.size)
                    assertEquals(1, snaps.single().relation.rowCount)
                }

                db.commitTx(tx)
            }

            db.liveIndex.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap ->
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

            db.liveIndex.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snapBefore ->
                val before = snapBefore.table(table).single()
                assertEquals(1, before.relation.rowCount)

                db.openTx(1, Instant.parse("2020-01-02T00:00:00Z")).use { tx2 ->
                    tx2.put(table, UUID.randomUUID())
                    db.commitTx(tx2)
                }

                assertEquals(1, before.relation.rowCount)

                db.liveIndex.openSnapshot(Instant.parse("2020-01-02T00:00:00Z")).use { snapAfter ->
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

                db.liveIndex.openSnapshot(emptyList(), tx).use { snap ->
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

            db.liveIndex.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap ->
                assertEquals(3, snap.table(table).single().relation.rowCount)
            }
        }
    }

    @Test
    fun `nextBlock removes tables`() {
        TestDb().use { db ->
            val table = TableRef("public", "test-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())
                db.commitTx(tx)
            }

            assertNotNull(db.liveIndex.table(table))

            db.liveIndex.finishBlock(db.bp, 0L)
            db.liveIndex.nextBlock()

            assertNull(db.liveIndex.table(table))
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

            db.liveIndex.openSnapshot(Instant.parse("2000-01-01T00:00:00Z")).use { snap ->
                val before = snap.table(table).single().relation.rowCount
                db.liveIndex.finishBlock(db.bp, 0L)
                assertEquals(before, snap.table(table).single().relation.rowCount)
                assertEquals(1, before)
            }
        }
    }

    // #5525 — between BlockUploader's addTries(L0_N) and nextBlock, the live-table for block N still
    // holds the rows the L0 trie now does. A snapshot opened in that window must not double-count:
    // the live-table is filtered, leaving L0 as the sole source of N's rows.
    @Test
    fun `snapshot filters live-table once L0 published`() {
        TestDb().use { db ->
            val table = TableRef("public", "dedup-table")

            db.openTx(0, Instant.parse("2020-01-01T00:00:00Z")).use { tx ->
                tx.put(table, UUID.randomUUID())
                db.commitTx(tx)
            }

            // observer tx forces a fresh snapshot capture each time — the cached sharedSnap was last
            // refreshed on commit, before our manual addTries below, so it wouldn't see the L0.
            db.openTx(1, Instant.parse("2020-01-01T00:00:01Z")).use { observerTx ->
                db.liveIndex.openSnapshot(emptyList(), observerTx).use { snap ->
                    assertEquals(1, snap.table(table).size, "live-table is the only source before finishBlock")
                    assertEquals(-1L, snap.trieCatSnap.l0MaxBlockIdx(table), "no L0 published yet")
                }

                // drive the BlockUploader flow up to (but not including) nextBlock
                for ((t, fb) in db.liveIndex.finishBlock(db.bp, 0L)) {
                    val trieDetails = TrieDetails.newBuilder()
                        .setTableName(t.schemaAndTable)
                        .setTrieKey(fb.trieKey)
                        .setDataFileSize(fb.dataFileSize)
                        .also { it.setTrieMetadata(fb.trieMetadata) }
                        .build()
                    db.trieCatalog.addTries(t, listOf(trieDetails), Instant.now())
                }

                db.liveIndex.openSnapshot(emptyList(), observerTx).use { snap ->
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

            db.liveIndex.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap0 ->
                // a second commit, with no read in between — must not eagerly rebuild the cached snapshot
                db.openTx(1, Instant.parse("2020-01-02T00:00:00Z")).use { tx ->
                    tx.put(table, UUID.randomUUID())
                    db.commitTx(tx)
                }

                db.liveIndex.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { reused ->
                    assertSame(snap0, reused, "a basis the cache already satisfies reuses it — the commit did not rebuild the snapshot")
                }

                db.liveIndex.openSnapshot(Instant.parse("2020-01-02T00:00:00Z")).use { rebuilt ->
                    assertNotSame(snap0, rebuilt, "a newer basis rebuilds the snapshot on demand")
                }
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

            db.liveIndex.openSnapshot(Instant.parse("2020-01-01T00:00:00Z")).use { snap ->
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
}
