package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.arrow.memory.RootAllocator
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.InMemoryLog
import xtdb.api.log.Log
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.SourceMessage
import xtdb.api.log.Watchers
import xtdb.api.storage.Storage
import xtdb.block.proto.TableBlock
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.trieMetadata
import xtdb.storage.BufferPool
import xtdb.table.TableRef
import xtdb.trie.TrieCatalog
import java.time.Instant
import java.time.InstantSource

class LeaderLogProcessorTest {

    private fun leaderProc(
        sourceLog: InMemoryLog<SourceMessage> = InMemoryLog(InstantSource.system(), 0),
        replicaLog: InMemoryLog<ReplicaMessage> = InMemoryLog(InstantSource.system(), 0),
        bufferPool: BufferPool = mockk(relaxed = true) { every { epoch } returns 0 },
        liveIndex: LiveIndex = mockk(relaxed = true),
        indexer: Indexer.ForDatabase = mockk(relaxed = true),
        blockCatalog: BlockCatalog = BlockCatalog("test", null),
        trieCatalog: TrieCatalog = mockk(relaxed = true),
        compactor: Compactor.ForDatabase = mockk(relaxed = true),
        sourceWatchers: Watchers = Watchers(-1),
        replicaWatchers: Watchers = Watchers(-1),
    ): LeaderLogProcessor {
        val tableCatalog = mockk<TableCatalog>(relaxed = true)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null)

        return LeaderLogProcessor(
            RootAllocator(), dbStorage, replicaProducer,
            dbState, indexer, sourceWatchers, replicaWatchers,
            emptySet(), null, blockUploader
        )
    }

    @Test
    fun `TriesAdded forwarded to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val trieCatalog = mockk<TrieCatalog>(relaxed = true)
        val lp = leaderProc(replicaLog = replicaLog, trieCatalog = trieCatalog)

        val tries = listOf(
            TrieDetails.newBuilder()
                .setTableName("public/foo")
                .setTrieKey("trie-key-1")
                .setDataFileSize(100)
                .setTrieMetadata(trieMetadata {})
                .build()
        )

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.TriesAdded(Storage.VERSION, 0, tries))
        ))

        verify { trieCatalog.addTries(any(), any(), any()) }
        assertTrue(replicaLog.latestSubmittedOffset >= 0, "replica log should have received a message")
    }

    @Test
    fun `FlushBlock triggers block finish when CAS matches`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { finishBlock(any()) } returns emptyMap()
            every { latestCompletedTx } returns null
        }
        val trieCatalog = mockk<TrieCatalog>(relaxed = true) {
            every { getPartitions(any()) } returns emptyList()
        }
        val tableCatalog = mockk<TableCatalog>(relaxed = true) {
            every { finishBlock(any(), any()) } returns emptyMap()
        }
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }
        val blockCatalog = BlockCatalog("test", null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null)

        val lp = LeaderLogProcessor(
            RootAllocator(), dbStorage, replicaProducer,
            dbState, mockk(relaxed = true), Watchers(-1), Watchers(-1),
            emptySet(), null, blockUploader
        )

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
        ))

        verify { liveIndex.finishBlock(0) }
        verify { liveIndex.nextBlock() }
        verify { compactor.signalBlock() }
        assertTrue(replicaLog.latestSubmittedOffset >= 0, "replica log should have block messages")
    }

    @Test
    fun `FlushBlock ignored when CAS does not match`() = runTest {
        val liveIndex = mockk<LiveIndex>(relaxed = true)
        val lp = leaderProc(liveIndex = liveIndex)

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(5))
        ))

        verify(exactly = 0) { liveIndex.finishBlock(any()) }
    }

    @Test
    fun `block finishing writes BlockBoundary + BlockUploaded to replica log`() = runTest {
        val replicaLog = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)
        val finishedBlock = LiveTable.FinishedBlock(
            vecTypes = emptyMap(),
            trieKey = "test-trie",
            dataFileSize = 42,
            rowCount = 10,
            trieMetadata = trieMetadata {},
            hllDeltas = emptyMap()
        )
        val tableRef = TableRef.parse("test", "public/foo")

        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { finishBlock(any()) } returns mapOf(tableRef to finishedBlock)
            every { latestCompletedTx } returns null
        }
        val trieCatalog = mockk<TrieCatalog>(relaxed = true) {
            every { getPartitions(any()) } returns emptyList()
        }
        val tableCatalog = mockk<TableCatalog>(relaxed = true) {
            every { finishBlock(any(), any()) } returns mapOf(
                tableRef to TableBlock.getDefaultInstance()
            )
        }
        val compactor = mockk<Compactor.ForDatabase>(relaxed = true)
        val bufferPool = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }
        val blockCatalog = BlockCatalog("test", null)
        val sourceLog = InMemoryLog<SourceMessage>(InstantSource.system(), 0)
        val dbState = DatabaseState("test", blockCatalog, tableCatalog, trieCatalog, liveIndex)
        val dbStorage = DatabaseStorage(sourceLog, replicaLog, bufferPool, null)
        val replicaProducer = replicaLog.openAtomicProducer("test-leader")
        val blockUploader = BlockUploader(dbStorage, dbState, compactor, null)

        val lp = LeaderLogProcessor(
            RootAllocator(), dbStorage, replicaProducer,
            dbState, mockk(relaxed = true), Watchers(-1), Watchers(-1),
            emptySet(), null, blockUploader
        )

        val now = Instant.now()
        lp.processRecords(listOf(
            Log.Record(0, 0, now, SourceMessage.FlushBlock(-1))
        ))

        val replicaMessages = mutableListOf<ReplicaMessage>()
        val consumer = replicaLog.openConsumer()
        val sub = consumer.tailAll(-1) { records ->
            replicaMessages.addAll(records.map { it.message })
        }

        Thread.sleep(200)
        sub.close()
        consumer.close()

        assertEquals(2, replicaMessages.size, "expected 2 replica messages, got: $replicaMessages")
        assertTrue(replicaMessages[0] is ReplicaMessage.BlockBoundary)
        assertTrue(replicaMessages[1] is ReplicaMessage.BlockUploaded)

        val boundary = replicaMessages[0] as ReplicaMessage.BlockBoundary
        assertEquals(0, boundary.blockIndex)

        val uploaded = replicaMessages[1] as ReplicaMessage.BlockUploaded
        assertEquals(0, uploaded.blockIndex)
        assertTrue(uploaded.tries.isNotEmpty(), "BlockUploaded should contain trie details")
    }
}
