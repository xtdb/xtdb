package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.error.Incorrect
import xtdb.api.log.Log
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.TxResult
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import io.micrometer.core.instrument.MeterRegistry

/**
 * An [ExternalSource.Factory] with no [ExternalSource.Registration] — i.e. exactly what a user
 * embedding XTDB supplies programmatically. It can't be serialised, and the ingest node never asks
 * it to be: there's no catalog and no "xtdb" primary to persist it as a secondary.
 *
 * On leadership it indexes [rowsToIndex] one tx each, counting down [indexed] as it goes.
 */
private class CountingExternalSource(
    private val rowsToIndex: Int,
    val indexed: CountDownLatch,
    val leaderFor: ConcurrentLinkedQueue<String>,
    private val dbName: String,
) : ExternalSource {

    class Factory(
        private val rows: Int,
        val indexed: CountDownLatch,
        val leaderFor: ConcurrentLinkedQueue<String>,
    ) : ExternalSource.Factory {
        override fun open(dbName: String, remotes: Map<RemoteAlias, Remote>, meterRegistry: MeterRegistry?) =
            CountingExternalSource(rows, indexed, leaderFor, dbName)
    }

    override suspend fun onPartitionAssigned(partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer) {
        leaderFor.add(dbName)
        repeat(rowsToIndex) {
            txIndexer.executeTx(externalSourceToken = null) { TxResult.Committed() }
            indexed.countDown()
        }
    }

    override fun close() = Unit
}

class IngestNodeTest {

    private fun extDbConfig(source: ExternalSource.Factory) =
        Database.Config(
            log = Log.inMemoryLog,
            storage = Storage.inMemory(),
            externalSource = source,
        )

    @Test
    fun `opens, runs the source on leadership, and closes cleanly — no catalog, no primary`() {
        val indexed = CountDownLatch(2)
        val leaderFor = ConcurrentLinkedQueue<String>()
        val source = CountingExternalSource.Factory(rows = 2, indexed = indexed, leaderFor = leaderFor)

        IngestNode.Config()
            .database("orders", extDbConfig(source))
            .open()
            .use {
                assertTrue(
                    indexed.await(30, TimeUnit.SECONDS),
                    "the programmatic source should be elected leader and index its rows",
                )
                assertEquals(listOf("orders"), leaderFor.toList())
            }
    }

    @Test
    fun `runs an independent source per database`() {
        val ordersIndexed = CountDownLatch(1)
        val paymentsIndexed = CountDownLatch(1)
        val leaderFor = ConcurrentLinkedQueue<String>()

        IngestNode.Config()
            .database("orders", extDbConfig(CountingExternalSource.Factory(1, ordersIndexed, leaderFor)))
            .database("payments", extDbConfig(CountingExternalSource.Factory(1, paymentsIndexed, leaderFor)))
            .open()
            .use {
                assertTrue(ordersIndexed.await(30, TimeUnit.SECONDS), "orders source ran")
                assertTrue(paymentsIndexed.await(30, TimeUnit.SECONDS), "payments source ran")
                assertEquals(setOf("orders", "payments"), leaderFor.toSet())
            }
    }

    @Test
    fun `rejects 'xtdb' as a database name up front`() {
        val source = CountingExternalSource.Factory(1, CountDownLatch(1), ConcurrentLinkedQueue())
        val config = IngestNode.Config().database("xtdb", extDbConfig(source))

        assertThrows(Incorrect::class.java) { config.open() }
    }
}
