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
import xtdb.compactor.RecencyPartition.WEEK
import xtdb.compactor.TemporalSplitting.*
import xtdb.database.DatabaseName
import xtdb.database.IDatabase
import xtdb.indexer.LogProcessor
import xtdb.log.proto.TrieDetails
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.symbol
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.Trie
import xtdb.trie.TrieCatalog
import xtdb.trie.TrieKey
import xtdb.util.StringUtil.asLexHex
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.requiringResolve
import xtdb.util.warn
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
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

enum class TemporalSplitting {
    CURRENT,
    HISTORICAL,
    BOTH
}

data class DriverConfig(
    val temporalSplitting: TemporalSplitting = CURRENT,
    val baseTime: Instant = Instant.parse("2020-01-01T00:00:00Z"),
    val blocksPerWeek: Long = 14
)

class MockDriver(
    val dispatcher: CoroutineDispatcher,
    val baseSeed: Int,
    config: DriverConfig
) : Factory {
    class AppendMessage(val triesAdded: TriesAdded, val msgTimestamp: Instant)

    private val temporalSplitting = config.temporalSplitting
    private val baseTime = config.baseTime
    private val blocksPerWeek = config.blocksPerWeek
    val trieKeyToFileSize = mutableMapOf<TrieKey, Long>()

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

        private fun Instant.plusWeeks(weeks: Long) =
            this.plus(weeks * 7, ChronoUnit.DAYS)

        private fun trieKeyRand(trieKey: Trie.Key) =
            Random(baseSeed xor trieKey.hashCode())

        private fun deterministicRecency(rand: Random, trieKey: Trie.Key): LocalDate {
            val blockIdx = trieKey.blockIndex
            val baseWeek = baseTime.plusWeeks(blockIdx / blocksPerWeek)

            val weekOffset = rand.nextLong(0, 2) // up to 2 weeks in the past
            return baseWeek.minus(weekOffset * 7, ChronoUnit.DAYS).asMicros.toPartition(WEEK)
        }

        private fun deterministicSizeSplit(rand: Random, size: Long) : Pair<Long, Long> {
            val part1 = rand.nextLong(100)
            val part2 = 100 - part1
            return Pair(size * part1 / 100, size * part2 / 100)
        }

        override fun executeJob(job: Job): TriesAdded {
            val trieKey = job.outputTrieKey
            val trieDetailsBuilder = TrieDetails.newBuilder()
                .setTableName(job.table.tableName)
            if (trieKey.level == 1L) {
                val addedTries = mutableListOf<TrieDetails>()
                val size = job.trieKeys.sumOf { trieKeyToFileSize[it]!! }
                val rand = trieKeyRand(trieKey)
                when(temporalSplitting) {
                    CURRENT -> {
                        trieKeyToFileSize[trieKey.toString()] = size
                        addedTries.add(
                            trieDetailsBuilder
                                .setTrieKey(trieKey.toString())
                                .setDataFileSize(size)
                                .build()
                            )
                    }
                    HISTORICAL -> {
                        val historicalTrieKey = Trie.Key(trieKey.level, deterministicRecency(rand, trieKey), trieKey.part, trieKey.blockIndex)
                        trieKeyToFileSize[historicalTrieKey.toString()] = size
                        addedTries.add(
                            trieDetailsBuilder
                                .setTrieKey(historicalTrieKey.toString())
                                .setDataFileSize(size)
                                .build()
                        )
                    }
                    BOTH -> {
                        val historicalTrieKey = Trie.Key(trieKey.level, deterministicRecency(rand, trieKey), trieKey.part, trieKey.blockIndex)
                        val (currentSize, historicalSize) = deterministicSizeSplit(rand, size)
                        trieKeyToFileSize[trieKey.toString()] = currentSize
                        trieKeyToFileSize[historicalTrieKey.toString()] = historicalSize
                        addedTries.addAll(listOf(
                            trieDetailsBuilder
                                .setTrieKey(trieKey.toString())
                                .setDataFileSize(currentSize)
                                .build(),
                            trieDetailsBuilder
                                .setTrieKey(historicalTrieKey.toString())
                                .setDataFileSize(historicalSize)
                                .build())
                        )
                    }
                }
                return TriesAdded(Storage.VERSION, 0, addedTries)
            } else {
                return TriesAdded(
                    Storage.VERSION, 0,
                    listOf(
                        TrieDetails.newBuilder()
                            .setTableName(job.table.tableName)
                            .setTrieKey(job.outputTrieKey.toString())
                            .setDataFileSize(101L * 1024L * 1024L)
                            .build()
                    )
                )
            }
        }

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
private const val testIterations = 10

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
                    testInstance.explicitSeed = withSeed.seed
                }
            }
        }
    }
}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithDriverConfig(
    val temporalSplitting: TemporalSplitting = TemporalSplitting.CURRENT,
    val baseTime: String = "2020-01-01T00:00:00Z",
    val blocksPerWeek: Long = 14
)

class DriverConfigExtension : BeforeEachCallback {
    override fun beforeEach(context: ExtensionContext) {
        val annotation = context.requiredTestMethod.getAnnotation(WithDriverConfig::class.java)
            ?: return

        val testInstance = context.requiredTestInstance
        if (testInstance !is SimulationTest) return

        testInstance.driverConfig = with(annotation) {
            DriverConfig(
                temporalSplitting = temporalSplitting,
                baseTime = Instant.parse(baseTime),
                blocksPerWeek = blocksPerWeek
            )
        }
    }
}

private val L0TrieKeys = sequence {
    var blockIndex = 0
    while (true) {
        yield("l00-rc-b" + blockIndex.asLexHex)
        blockIndex++
    }
}

@ExtendWith(SeedExceptionWrapper::class, SeedExtension::class, DriverConfigExtension::class)
class SimulationTest {
    var currentSeed: Int = 0
    var explicitSeed: Int? = null
    var driverConfig: DriverConfig = DriverConfig()
    private lateinit var mockDriver: MockDriver
    private lateinit var jobCalculator: Compactor.JobCalculator
    private lateinit var compactor: Compactor.Impl
    private lateinit var trieCatalog: TrieCatalog
    private lateinit var db: MockDb
    private lateinit var dispatcher: CoroutineDispatcher

    @BeforeEach
    fun setUp() {
        setLogLevel.invoke("xtdb.compactor".symbol, logLevel)
        currentSeed = explicitSeed ?: Random.nextInt()
        dispatcher = DeterministicDispatcher(currentSeed)
        mockDriver = MockDriver(dispatcher, currentSeed, driverConfig)
        jobCalculator = createJobCalculator.invoke() as Compactor.JobCalculator
        compactor = Compactor.Impl(mockDriver, null, jobCalculator, false, 2, dispatcher)
        trieCatalog = createTrieCatalog.invoke(mutableMapOf<Any, Any>(), 100 * 1024 * 1024) as TrieCatalog
        db = MockDb("xtdb", trieCatalog)
    }

    @AfterEach
    fun tearDown() {
        driverConfig = DriverConfig()
        explicitSeed = null
    }

    private fun addL0s(tableRef: TableRef, l0s: List<TrieDetails>) {
        l0s.forEach {
            mockDriver.trieKeyToFileSize[it.trieKey.toString()] = it.dataFileSize
        }
        trieCatalog.addTries(tableRef, l0s, Instant.now())
    }


    @RepeatedTest(testIterations)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun singleL0Compaction() {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0Trie = buildTrieDetails(docsTable.tableName, L0TrieKeys.first())

        addL0s(docsTable, listOf(l0Trie))

        compactor.openForDatabase(db).use {
            it.compactAll()
        }

        Assertions.assertEquals(
            listOf("l00-rc-b00", "l01-rc-b00"),
            trieCatalog.listAllTrieKeys(docsTable),
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
            addL0s(
                table,
                listOf(
                    buildTrieDetails(table.tableName, "l00-rc-b00", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b01", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b02", 10 * 1024)
                )
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
           addL0s(
                table,
                listOf(
                    buildTrieDetails(table.tableName, "l00-rc-b03", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b04", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b05", 10 * 1024)
                )
            )

            it.compactAll()

            Assertions.assertEquals(
                listOf(
                    "l00-rc-b00", "l00-rc-b01", "l00-rc-b02", "l00-rc-b03", "l00-rc-b04", "l00-rc-b05",
                    "l01-rc-b00", "l01-rc-b01", "l01-rc-b02", "l01-rc-b03", "l01-rc-b04", "l01-rc-b05",
                ),
                trieCatalog.listAllTrieKeys(table)
            )
        }
    }

    @Test
    @WithSeed(-1754611144)
    fun multipleL0ToL1CompactionIssue() {
        multipleL0ToL1Compaction()
    }

    @Test
    @WithDriverConfig(temporalSplitting = CURRENT)
    fun biggerCompactorRun() {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0tries = L0TrieKeys.take(100).map { buildTrieDetails(docsTable.tableName, it, 10L * 1024L * 1024L) }

        addL0s(docsTable, l0tries.toList())

        var currentTries: List<TrieKey>

        compactor.openForDatabase(db).use {
            do {
                currentTries = trieCatalog.listAllTrieKeys(docsTable)
                it.compactAll()
            } while( currentTries.size != trieCatalog.listAllTrieKeys(docsTable).size)
        }

        Assertions.assertEquals(
            208,
            trieCatalog.listAllTrieKeys(docsTable).size
        )
    }
}