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
import xtdb.XtdbInternal
import xtdb.api.TableRef
import xtdb.api.Xtdb
import xtdb.api.error.Incorrect
import xtdb.api.log.Log
import xtdb.api.storage.Storage
import xtdb.tx.TxOp
import xtdb.database.Database
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
                val flush = primary.sendFlushBlockMessage()
                runBlocking { withTimeout(10.seconds) { primary.partitions.single().watchers.awaitSource(flush.msgId) } }

                assertNull(primary.ingestionError)
                assertEquals(0L, primary.tableCatalog.currentBlockIndex)
                assertEquals(3, primary.tableCatalog.secondaryDatabases.getValue("parts").partitions)
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
}
