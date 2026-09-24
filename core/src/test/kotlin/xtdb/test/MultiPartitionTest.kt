package xtdb.test

import clojure.lang.Keyword
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import xtdb.XtdbInternal
import xtdb.api.TableRef
import xtdb.api.Xtdb
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported
import xtdb.api.log.Log
import xtdb.api.storage.Storage
import xtdb.tx.TxOp
import xtdb.database.Database
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

class MultiPartitionTest {

    private fun dbConfig() = Database.Config(log = Log.inMemoryLog, storage = Storage.inMemory())

    private fun Xtdb.attach(
        dbName: String, source: InMemoryExternalSource, partitions: Int, config: Database.Config = dbConfig(),
    ): Database {
        connect().use { conn ->
            val tx = conn.attachDb(dbName, config.externalSource(source.factory).partitions(partitions))
            check(tx.committed) { "attach aborted: ${tx.error}" }
        }

        return (this as XtdbInternal).dbCatalog.databaseOrNull(dbName)!!
    }

    @Test
    fun `a database attached with N partitions has one partition per configured partition`() {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val db = node.attach("parts", source, partitions = 3)

                assertEquals(listOf(0, 1, 2), db.partitions.map { it.partition })
            }
        }
    }

    @Test
    fun `each partition holds its own storage and live index`() {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val partitions = node.attach("parts", source, partitions = 3).partitions

                // distinct instances rather than one shared behind three handles — a partition reading or
                // flushing must not move another's watermark
                assertEquals(3, partitions.map { it.bufferPool }.distinct().size)
                assertEquals(3, partitions.map { it.liveIndex }.distinct().size)
                assertEquals(3, partitions.map { it.watchers }.distinct().size)

                assertNotSame(partitions[0].tableCatalog, partitions[1].tableCatalog)
            }
        }
    }

    @Test
    fun `the primary records a multi-partition secondary when it cuts a block`() {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                node.attach("parts", source, partitions = 3)

                val primary = (node as XtdbInternal).dbCatalog.primary
                val flush = primary.sendFlushBlockMessage(0)
                runBlocking { withTimeout(10.seconds) { primary.partitions.single().watchers.awaitSource(flush.msgId) } }

                assertNull(primary.ingestionError)
                assertEquals(0L, primary.tableCatalog.currentBlockIndex)
                assertEquals(3, primary.tableCatalog.secondaryDatabases.getValue("parts").partitions)
            }
        }
    }

    @Test
    fun `a multi-partition database survives a restart, and each partition resumes after its own token`(
        @TempDir dir: Path,
    ) = runBlocking {
        fun openNode() = Xtdb.openNode {
            log(Log.localLog(dir.resolve("log")))
            storage(Storage.local(dir.resolve("storage")))
        }

        InMemoryExternalSource(partitions = 3).use { source ->
            openNode().use { node ->
                node.attach(
                    "parts", source, partitions = 3,
                    config = Database.Config(
                        log = Log.localLog(dir.resolve("parts-log")),
                        storage = Storage.local(dir.resolve("parts-storage")),
                    ),
                )

                source.publish(partition = 1)
                val parts = (node as XtdbInternal).dbCatalog.databaseOrNull("parts")!!
                withTimeout(10.seconds) { parts.partitions[1].watchers.awaitTx(0) }

                val primary = node.dbCatalog.primary
                val flush = primary.sendFlushBlockMessage(0)
                withTimeout(10.seconds) { primary.partitions.single().watchers.awaitSource(flush.msgId) }
            }

            source.publish(partition = 2)

            openNode().use { node ->
                val parts = (node as XtdbInternal).dbCatalog.databaseOrNull("parts")!!
                assertEquals(3, parts.partitions.size)

                withTimeout(10.seconds) { parts.partitions[2].watchers.awaitTx(0) }

                assertEquals(0L, parts.partitions[1].liveIndex.latestCompletedTx?.txId, "partition 1 didn't re-index its message")
                assertEquals(0L, parts.partitions[2].liveIndex.latestCompletedTx?.txId)
            }
        }
    }

    @Test
    fun `a message published to a partition is indexed by that partition alone`() = runBlocking {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val partitions = node.attach("parts", source, partitions = 3).partitions

                source.publish(partition = 1)
                withTimeout(10.seconds) { partitions[1].watchers.awaitTx(0) }

                assertEquals(0L, partitions[1].liveIndex.latestCompletedTx?.txId)
                assertNull(partitions[0].liveIndex.latestCompletedTx, "partition 0 indexed nothing")
                assertNull(partitions[2].liveIndex.latestCompletedTx, "partition 2 indexed nothing")
            }
        }
    }

    @Test
    fun `partitions record their transactions in separate tx tables`() = runBlocking {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val partitions = node.attach("parts", source, partitions = 3).partitions

                // both are tx 0 of their own partition — one table would read these as one entity at two times
                source.publish(partition = 1)
                source.publish(partition = 2)
                withTimeout(10.seconds) {
                    partitions[1].watchers.awaitTx(0)
                    partitions[2].watchers.awaitTx(0)
                }

                assertEquals(TableRef("xt", "txs_1"), partitions[1].state.txsTable)
                assertEquals(TableRef("xt", "txs_2"), partitions[2].state.txsTable)

                assertTrue(
                    TableRef("xt", "txs_1") in partitions[1].liveIndex.openSnapshot(null).use { it.tableInfo },
                    "partition 1's tx row went to its own table"
                )
                assertFalse(
                    TableRef("xt", "txs_1") in partitions[2].liveIndex.openSnapshot(null).use { it.tableInfo },
                    "and not into partition 2's live index"
                )
            }
        }
    }

    @Test
    fun `a transaction executed against an external-source database is refused before its tx table is read`() {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                node.attach("parts", source, partitions = 3)

                val ex = assertThrows<Incorrect> {
                    node.connect("parts").use { it.executeTx(listOf(TxOp.Sql("INSERT INTO docs (_id) VALUES (1)"))) }
                }
                assertEquals(Keyword.intern("xtdb", "submit-tx-to-external-source-db"), ex.data.valAt(Keyword.intern("xtdb.error", "code")))
            }
        }
    }

    @Test
    fun `a single-partition database keeps xt txs unchanged`() {
        Xtdb.openNode().use { node ->
            val db = (node as XtdbInternal).dbCatalog.databaseOrNull("xtdb")!!
            assertEquals(TableRef("xt", "txs"), db.partitions.single().state.txsTable)
        }
    }

    @Test
    fun `a flush cuts the block of the partition it names, and no other`() = runBlocking {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val db = node.attach("parts", source, partitions = 3)
                val partitions = db.partitions

                for (p in 0..2) source.publish(partition = p)
                withTimeout(10.seconds) { partitions.forEach { it.watchers.awaitTx(0) } }

                val flush = db.sendFlushBlockMessage(1)
                withTimeout(10.seconds) { partitions[1].watchers.awaitSource(flush.msgId) }

                assertEquals(0L, partitions[1].tableCatalog.currentBlockIndex)
                assertNull(partitions[0].tableCatalog.currentBlockIndex, "partition 0 did not cut")
                assertNull(partitions[2].tableCatalog.currentBlockIndex, "partition 2 did not cut")
            }
        }
    }

    @Test
    fun `awaiting a bare tx-id is refused above one partition`() {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val db = node.attach("parts", source, partitions = 3)

                val ex = assertThrows<Unsupported> { db.awaitTxBlocking(0) }
                assertEquals(Keyword.intern("xtdb", "await-tx-multi-partition"), ex.data.valAt(Keyword.intern("xtdb.error", "code")))
            }
        }
    }

    @Test
    fun `sync leaves every partition caught up with its own slice of the log`() = runBlocking {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val db = node.attach("parts", source, partitions = 3)

                // the last partition only, so that a sync watching the first has nothing of its own to wait for
                val flush = db.sendFlushBlockMessage(2)

                withTimeout(10.seconds) { db.sync() }

                // against the message we sent, not against the log's head: a block cut publishes its tries
                // back to the source log, so the head moves under a live database
                assertTrue(
                    db.partitions[2].watchers.latestSourceMsgId >= flush.msgId,
                    "partition 2 processed the flush that sync was waiting for"
                )
            }
        }
    }

    @Test
    fun `a partition's data lands in its own storage subtree`(@TempDir storagePath: Path) = runBlocking {
        InMemoryExternalSource(partitions = 3).use { source ->
            Xtdb.openNode().use { node ->
                val db = node.attach(
                    "parts", source, partitions = 3,
                    config = Database.Config(log = Log.inMemoryLog, storage = Storage.local(storagePath)),
                )

                // an empty transaction still writes its own xt.txs row, which is what the cut then persists
                for (p in 0..2) source.publish(partition = p)
                withTimeout(10.seconds) { db.partitions.forEach { it.watchers.awaitTx(0) } }

                db.sendFlushBlockMessage()
                withTimeout(10.seconds) { db.sync() }

                // every partition's root is created at open, so only files tell them apart
                val files = Files.walk(storagePath).use { paths ->
                    paths.filter(Files::isRegularFile).map { storagePath.relativize(it).toString() }.toList()
                }

                // each under its own: three partitions writing to one root would leave the other two empty
                for (p in 0..2) {
                    assertTrue(files.any { it.startsWith("parts/$p/") }, "partition $p wrote under its own root: $files")
                }
            }
        }
    }
}
