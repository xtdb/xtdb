package xtdb.compactor

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler
import xtdb.api.log.Log
import xtdb.api.log.Log.Message.TriesAdded
import xtdb.api.storage.Storage
import xtdb.arrow.unsupported
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor.Driver
import xtdb.compactor.Compactor.Driver.Factory
import xtdb.compactor.Compactor.Job
import xtdb.database.DatabaseName
import xtdb.database.IDatabase
import xtdb.indexer.LogProcessor
import xtdb.log.proto.TrieDetails
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.symbol
import xtdb.table.TableRef
import xtdb.trie.TrieCatalog
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.requiringResolve
import xtdb.util.warn
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

class MockDb(override val name: DatabaseName, override val trieCatalog: TrieCatalog) : IDatabase {
    override val allocator: BufferAllocator get() = unsupported("allocator")
    override val blockCatalog: BlockCatalog get() = unsupported("blockCatalog")
    override val tableCatalog: TableCatalog get() = unsupported("tableCatalog")
    override val log: Log get() = unsupported("log")
    override val bufferPool: BufferPool get() = unsupported("bufferPool")
    override val metadataManager: PageMetadata.Factory get() = unsupported("metadataManager")

    override val logProcessor: LogProcessor get() = unsupported("logProcessor")
    override val compactor: Compactor.ForDatabase get() = unsupported("compactor")
}

private val LOGGER = MockDriver::class.logger

class MockDriver(val dispatcher: CoroutineDispatcher) : Factory {
    class AppendMessage(val triesAdded: TriesAdded, val msgTimestamp: Instant)

    override fun create(db: IDatabase) = ForDatabase(db)

    inner class ForDatabase(db: IDatabase) : Driver {
        val channel = Channel<AppendMessage>(UNLIMITED)
        val trieCatalog = db.trieCatalog

        val job = CoroutineScope(dispatcher).launch {
            for (msg in channel) {
                msg.triesAdded.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                    trieCatalog.addTries(TableRef.parse(db.name, tableName), tries, msg.msgTimestamp)
                }
            }
        }

        override fun executeJob(job: Job) =
            TriesAdded(
                Storage.VERSION, 0,
                listOf(
                    TrieDetails.newBuilder()
                        .setTableName(job.table.tableName)
                        .setTrieKey(job.outputTrieKey.toString())
                        .setDataFileSize(100 * 1024L * 1024L)
                        .build()
                )
            )

        var logOffset = 0L

        override suspend fun appendMessage(triesAdded: TriesAdded): Log.MessageMetadata {
            val logTimestamp = Instant.now()
            channel.send(AppendMessage(triesAdded, logTimestamp))
            return Log.MessageMetadata(logOffset++, logTimestamp)
        }

        override fun close() = close(null)

        fun close(e: Throwable?) {
            channel.close(e)
            runBlocking {
                job.cancelAndJoin()
            }
        }
    }
}

private fun buildTrieDetails(tableName: String, trieKey: String, dataFileSize: Long = 1024L): TrieDetails =
    TrieDetails.newBuilder()
        .setTableName(tableName)
        .setTrieKey(trieKey)
        .setDataFileSize(dataFileSize)
        .build()

class SeedExceptionWrapper : TestExecutionExceptionHandler {
    override fun handleTestExecutionException(context: ExtensionContext, throwable: Throwable) {
        val testInstance = context.testInstance.orElse(null)
        if (testInstance is SimulationTest) {
            val seed = testInstance.currentSeed
            // Wrap and rethrow with added context
            LOGGER.warn(throwable, "Test failed with seed: ${testInstance.currentSeed}")
            throw AssertionError("Test threw an exception (seed=$seed)", throwable)
        }
    }
}

// Settings used by all tests in this class
private const val logLevel = "DEBUG"
private const val testIterations = 1

// Clojure interop to get at internal functions
private val setLogLevel = requiringResolve("xtdb.logging/set-log-level!")
private val createJobCalculator = requiringResolve("xtdb.compactor/->JobCalculator")
private val createTrieCatalog = requiringResolve("xtdb.trie-catalog/->TrieCatalog")

class DeterministicDispatcher(seed: Int) : CoroutineDispatcher() {

    private data class DispatchJob(val context: CoroutineContext, val block: Runnable)

    private val rand = Random(seed)

    private val jobs = mutableSetOf<DispatchJob>()

    @Volatile
    private var running = false

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        jobs.add(DispatchJob(context, block))

        if (!running) {
            running = true
            while (true) {
                val job = jobs.randomOrNull(rand) ?: break
                jobs.remove(job)
                job.block.run()
            }
            running = false
        }
    }
}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithSeed(val seed: Int)

class SeedExtension : BeforeEachCallback {
    override fun beforeEach(context: ExtensionContext) {
        val annotation = context.requiredTestMethod
            .getAnnotation(WithSeed::class.java)

        annotation?.let { withSeed ->
            context.requiredTestInstance.let { testInstance ->
                if (testInstance is SimulationTest) {
                    testInstance.currentSeed = withSeed.seed
                }
            }
        }
    }
}

@ExtendWith(SeedExceptionWrapper::class)
class SimulationTest {
    var currentSeed: Int = 0
    private lateinit var mockDriver: MockDriver
    private lateinit var jobCalculator: Compactor.JobCalculator
    private lateinit var compactor: Compactor.Impl
    private lateinit var trieCatalog: TrieCatalog
    private lateinit var db: MockDb
    private lateinit var dispatcher: CoroutineDispatcher

    @BeforeEach
    fun setUp() {
        setLogLevel.invoke("xtdb.compactor".symbol, logLevel)
        currentSeed = -194139152 // Random.nextInt()
        dispatcher =  DeterministicDispatcher(currentSeed)
        mockDriver = MockDriver(dispatcher)
        jobCalculator = createJobCalculator.invoke() as Compactor.JobCalculator
        compactor = Compactor.Impl(mockDriver, null, jobCalculator, false, 2, dispatcher)
        trieCatalog = createTrieCatalog.invoke(mutableMapOf<Any, Any>(), 100 * 1024 * 1024) as TrieCatalog
        db = MockDb("xtdb", trieCatalog)
    }

    @RepeatedTest(testIterations)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun singleL0Compaction() {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0Trie = buildTrieDetails(docsTable.tableName, "l00-rc-b01")

        trieCatalog.addTries(docsTable, listOf(l0Trie), Instant.now())

        compactor.openForDatabase(db).use {
            it.compactAll()
        }

        Assertions.assertEquals(
            listOf("l00-rc-b01", "l01-rc-b01"),
            trieCatalog.listAllTrieKeys(docsTable)
        )
    }

    @Test
    @WithSeed(-1748393987)
    fun singleL0CompactionNotHanging() {
        singleL0Compaction()
    }

    @RepeatedTest(testIterations)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun multipleL0ToL1Compaction() {
        val table = TableRef("xtdb", "public", "docs")

        compactor.openForDatabase(db).use {
            // Round 1: Add 3 L0 tries and compact
            trieCatalog.addTries(
                table,
                listOf(
                    buildTrieDetails(table.tableName, "l00-rc-b00", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b01", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b02", 10 * 1024)
                ),
                Instant.now()
            )

            it.compactAll()

            Assertions.assertEquals(
                listOf(
                    "l00-rc-b00", "l00-rc-b01", "l00-rc-b02",
                    "l01-rc-b00", "l01-rc-b01", "l01-rc-b02"
                ),
                trieCatalog.listAllTrieKeys(table)
            )

            // Round 2: Add 3 more L0 tries and compact
            trieCatalog.addTries(
                table,
                listOf(
                    buildTrieDetails(table.tableName, "l00-rc-b03", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b04", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b05", 10 * 1024)
                ),
                Instant.now()
            )

            it.compactAll()

            Assertions.assertEquals(
                listOf(
                    "l00-rc-b00", "l00-rc-b01", "l00-rc-b02", "l00-rc-b03", "l00-rc-b04", "l00-rc-b05",
                    "l01-rc-b00", "l01-rc-b01", "l01-rc-b02", "l01-rc-b03", "l01-rc-b04", "l01-rc-b05",
                    "l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03"
                ),
                trieCatalog.listAllTrieKeys(table)
            )
        }
    }

}
