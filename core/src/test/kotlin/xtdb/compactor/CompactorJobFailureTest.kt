package xtdb.compactor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.api.log.SourceMessage.TriesAdded
import xtdb.api.log.Watchers
import xtdb.database.DatabaseLogs
import xtdb.database.PartitionState
import xtdb.database.PartitionStorage
import xtdb.storage.MemoryStorage
import xtdb.table.fromSchemaAndTable
import xtdb.trie.Trie
import xtdb.trie.TrieCatalog

class CompactorJobFailureTest {

    private val jobError = IllegalStateException("job failed")

    private val job = object : Compactor.Job {
        override val table = fromSchemaAndTable("foo")
        override val trieKeys = listOf(Trie.l0Key(0).toString())
        override val part = ByteArray(0)
        override val outputTrieKey = Trie.Key(1, null, null, 0)
        override val partitionedByRecency = false
    }

    private val failingDriverFactory = object : Compactor.Driver.Factory {
        override fun create(
            allocator: BufferAllocator, partitionStorage: PartitionStorage,
            partitionState: PartitionState, watchers: Watchers
        ) = object : Compactor.Driver {
            override suspend fun executeJob(job: Compactor.Job): TriesAdded = throw jobError
            override suspend fun publishTries(triesAdded: TriesAdded) = Unit
            override fun close() = Unit
        }
    }

    private lateinit var allocator: BufferAllocator
    private lateinit var storage: MemoryStorage
    private lateinit var scope: CoroutineScope
    private lateinit var compactor: Compactor.ForDatabase

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        storage = MemoryStorage(allocator, epoch = 0)
        scope = CoroutineScope(Dispatchers.Default)

        val jobCalculator = object : Compactor.JobCalculator {
            override fun availableJobs(trieCatalog: TrieCatalog) = listOf(job)
        }

        compactor = Compactor.Impl(failingDriverFactory, null, jobCalculator, false, 1)
            .openForDatabase(
                scope, allocator,
                PartitionStorage(DatabaseLogs(null, null), storage, null),
                PartitionState(null, createTrieCatalog(), null),
                Watchers(latestTxId = -1, latestSourceMsgId = -1)
            )
    }

    @AfterEach
    fun tearDown() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
        compactor.close()
        storage.close()
        allocator.close()
    }

    // coroutines' stack-trace recovery rethrows a copy, with the original as its cause
    private fun Throwable.isOrCausedBy(expected: Throwable) =
        generateSequence(this) { it.cause }.any { it === expected }

    @Test
    fun `compactAll fails with a failed job's error`() = runBlocking {
        val e = assertThrows<IllegalStateException> { withTimeout(30_000) { compactor.compactAll() } }
        assertTrue(e.isOrCausedBy(jobError))
    }

    @Test
    fun `a later compactAll fails with the same error`() = runBlocking {
        assertThrows<IllegalStateException> { withTimeout(30_000) { compactor.compactAll() } }

        val e = assertThrows<IllegalStateException> { withTimeout(30_000) { compactor.compactAll() } }
        assertTrue(e.isOrCausedBy(jobError))
    }
}
