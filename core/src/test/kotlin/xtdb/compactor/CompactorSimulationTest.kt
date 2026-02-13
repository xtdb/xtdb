package xtdb.compactor

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableSharedFlow
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import xtdb.SimulationTestBase
import xtdb.SimulationTestUtils.Companion.L0TrieKeys
import xtdb.SimulationTestUtils.Companion.L1TrieKeys
import xtdb.SimulationTestUtils.Companion.addTriesToBufferPool
import xtdb.SimulationTestUtils.Companion.buildTrieDetails
import xtdb.SimulationTestUtils.Companion.createJobCalculator
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.SimulationTestUtils.Companion.prefix
import xtdb.SimulationTestUtils.Companion.setLogLevel
import xtdb.WithSeed
import xtdb.api.log.Log
import xtdb.api.log.Log.Message.TriesAdded
import xtdb.api.storage.Storage
import xtdb.compactor.Compactor.Driver
import xtdb.compactor.Compactor.Driver.Factory
import xtdb.compactor.Compactor.Job
import xtdb.compactor.RecencyPartition.WEEK
import xtdb.compactor.TemporalSplitting.*
import xtdb.database.DatabaseName
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.storage.MemoryStorage
import xtdb.symbol
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.Trie
import xtdb.trie.TrieCatalog
import xtdb.trie.TrieKey
import xtdb.util.logger
import xtdb.util.safeMap
import xtdb.util.useAll
import xtdb.util.debug
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.random.Random

class MockDb(
    val name: DatabaseName,
    val trieCatalog: TrieCatalog,
    val bufferPool: BufferPool,
    val compactor: Compactor,
) {
    val dbStorage: DatabaseStorage get() = DatabaseStorage(null, null, bufferPool, null)
    val dbState: DatabaseState get() = DatabaseState(name, null, null, trieCatalog, null)
}

private val LOGGER = CompactorMockDriver::class.logger

enum class TemporalSplitting {
    CURRENT,
    HISTORICAL,
    BOTH
}

data class CompactorDriverConfig(
    val temporalSplitting: TemporalSplitting = CURRENT,
    val baseTime: Instant = Instant.parse("2020-01-01T00:00:00Z"),
    val blocksPerWeek: Long = 140
)

class CompactorMockDriver(
    val dispatcher: CoroutineDispatcher,
    val baseSeed: Int,
    config: CompactorDriverConfig
) : Factory {
    class AppendMessage(val triesAdded: TriesAdded, val msgTimestamp: Instant, val systemId: Int = 0)

    private val temporalSplitting = config.temporalSplitting
    private val baseTime = config.baseTime
    private val blocksPerWeek = config.blocksPerWeek
    // Key is "tableName:trieKey" to avoid collisions between tables with the same trie key
    val trieKeyToFileSize = ConcurrentHashMap<String, Long>()

    fun fileSizeKey(tableName: String, trieKey: String) = "$tableName:$trieKey"
    val sharedFlow = MutableSharedFlow<AppendMessage>(extraBufferCapacity = Int.MAX_VALUE)
    var nextSystemId = 0

    override fun create(allocator: BufferAllocator, dbStorage: DatabaseStorage, dbState: DatabaseState): Driver {
        val systemId = nextSystemId++
        return ForDatabase(dbStorage, dbState, systemId)
    }

    inner class ForDatabase(val dbStorage: DatabaseStorage, val dbState: DatabaseState, val systemId: Int) : Driver {
        val trieCatalog = dbState.trieCatalog
        val bufferPool = dbStorage.bufferPool

        val job = CoroutineScope(dispatcher).launch {
            sharedFlow.collect { msg ->
                val trieKeys = msg.triesAdded.tries.map { it.trieKey }
                LOGGER.debug("[channel msg received] systemId=$systemId received ${trieKeys.size} tries: $trieKeys")
                yield() // force suspension mid-message processing
                msg.triesAdded.tries.groupBy { it.tableName }.forEach { (tableName, tries) ->
                    val tableRef = TableRef.parse(dbState.name, tableName)
                    addTriesToBufferPool(bufferPool, tableRef, tries)
                    trieCatalog.addTries(tableRef, tries, msg.msgTimestamp)
                }

                LOGGER.debug("[channel msg processed] systemId=$systemId added ${trieKeys.size} tries to catalog: $trieKeys")
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

        override suspend fun executeJob(job: Job): TriesAdded {
            val tableName = job.table.tableName
            LOGGER.debug("[executeJob started] systemId=$systemId table=$tableName job.trieKeys=${job.trieKeys} job.outputTrieKey=${job.outputTrieKey}")
            yield() // Force suspension after executeJob has started
            val trieKey = job.outputTrieKey
            val trieDetailsBuilder = TrieDetails.newBuilder()
                .setTableName(tableName)
            val result = if (trieKey.level == 1L) {
                val addedTries = mutableListOf<TrieDetails>()
                val size = job.trieKeys.sumOf { trieKeyToFileSize[fileSizeKey(tableName, it)]!! }
                val rand = trieKeyRand(trieKey)
                when(temporalSplitting) {
                    CURRENT -> {
                        trieKeyToFileSize[fileSizeKey(tableName, trieKey.toString())] = size
                        addedTries.add(
                            trieDetailsBuilder
                                .setTrieKey(trieKey.toString())
                                .setDataFileSize(size)
                                .build()
                            )
                    }
                    HISTORICAL -> {
                        val historicalTrieKey = Trie.Key(trieKey.level, deterministicRecency(rand, trieKey), trieKey.part, trieKey.blockIndex)
                        trieKeyToFileSize[fileSizeKey(tableName, historicalTrieKey.toString())] = size
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
                        trieKeyToFileSize[fileSizeKey(tableName, trieKey.toString())] = currentSize
                        trieKeyToFileSize[fileSizeKey(tableName, historicalTrieKey.toString())] = historicalSize
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
                addTriesToBufferPool(bufferPool, job.table, addedTries)
                TriesAdded(Storage.VERSION, 0, addedTries)
            } else {
                val addedTries = listOf(
                    TrieDetails.newBuilder()
                        .setTableName(job.table.tableName)
                        .setTrieKey(job.outputTrieKey.toString())
                        .setDataFileSize(100L * 1024L * 1024L)
                        .build()
                )

                addTriesToBufferPool(bufferPool, job.table, addedTries)
                TriesAdded(Storage.VERSION, 0, addedTries)


            }
            yield() // Force suspension before returning result
            LOGGER.debug("[executeJob completed] systemId=$systemId table=${job.table.tableName} job.outputTrieKey=${job.outputTrieKey}")
            return result
        }

        var logOffset = 0L

        override suspend fun appendMessage(triesAdded: TriesAdded): Log.MessageMetadata {
            LOGGER.debug("[appendMessage started] systemId=$systemId offset=$logOffset tries=${triesAdded.tries.map { it.trieKey }}")
            yield() // Force suspension after appendMessage started
            val logTimestamp = Instant.now()
            sharedFlow.emit(AppendMessage(triesAdded, logTimestamp, systemId))
            LOGGER.debug("[appendMessage completed] systemId=$systemId offset=$logOffset sent to channel")
            return Log.MessageMetadata(logOffset++, logTimestamp)
        }

        override fun close() {
            runBlocking {
                job.cancelAndJoin()
            }
        }
    }
}

// Settings used by all tests in this class
private const val logLevel = "WARN"

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithCompactorDriverConfig(
    val temporalSplitting: TemporalSplitting = TemporalSplitting.CURRENT,
    val baseTime: String = "2020-01-01T00:00:00Z",
    val blocksPerWeek: Long = 14
)

@ParameterizedTest(name = "[iteration {0}]")
@MethodSource("xtdb.SimulationTestBase#iterationSource")
annotation class RepeatableSimulationTest

class DriverConfigExtension : BeforeEachCallback {
    override fun beforeEach(context: ExtensionContext) {
        val annotation = context.requiredTestMethod.getAnnotation(WithCompactorDriverConfig::class.java)
            ?: return

        val testInstance = context.requiredTestInstance
        if (testInstance !is CompactorSimulationTest) return

        testInstance.driverConfig = with(annotation) {
            CompactorDriverConfig(
                temporalSplitting = temporalSplitting,
                baseTime = Instant.parse(baseTime),
                blocksPerWeek = blocksPerWeek
            )
        }
    }
}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithNumberOfSystems(val numberOfSystems: Int)

class NumberOfSystemsExtension : BeforeEachCallback {
    override fun beforeEach(context: ExtensionContext) {
        val annotation = context.requiredTestMethod.getAnnotation(WithNumberOfSystems::class.java)
            ?: return

        val testInstance = context.requiredTestInstance
        if (testInstance !is CompactorSimulationTest) return

        testInstance.numberOfSystems = annotation.numberOfSystems
    }
}

@Tag("property")
@ExtendWith(DriverConfigExtension::class, NumberOfSystemsExtension::class)
class CompactorSimulationTest : SimulationTestBase() {
    var driverConfig: CompactorDriverConfig = CompactorDriverConfig()
    var numberOfSystems: Int = 1
    private lateinit var allocator: BufferAllocator
    private lateinit var sharedBufferPool: BufferPool
    private lateinit var mockDriver: CompactorMockDriver
    private lateinit var jobCalculator: Compactor.JobCalculator
    private lateinit var dbs: List<MockDb>

    @BeforeEach
    fun setUp() {
        super.setUpSimulation()
        setLogLevel.invoke("xtdb.compactor".symbol, logLevel)
        mockDriver = CompactorMockDriver(dispatcher, currentSeed, driverConfig)
        jobCalculator = createJobCalculator()
        allocator = RootAllocator()

        // Create a single shared buffer pool for all systems
        sharedBufferPool = MemoryStorage(allocator, epoch = 0)

        dbs = List(numberOfSystems) {
            MockDb(
                name = "xtdb",
                trieCatalog = createTrieCatalog(),
                bufferPool = sharedBufferPool,
                compactor = Compactor.Impl(mockDriver, null, jobCalculator, false, 2, dispatcher),
            )
        }
    }

    @AfterEach
    fun tearDown() {
        driverConfig = CompactorDriverConfig()
        sharedBufferPool.close()
        allocator.close()
        super.tearDownSimulation()
    }

    private fun addL0s(tableRef: TableRef, l0s: List<TrieDetails>) {
        l0s.forEach {
            mockDriver.trieKeyToFileSize[mockDriver.fileSizeKey(tableRef.tableName, it.trieKey.toString())] = it.dataFileSize
        }
        dbs.forEach { db ->
            addTriesToBufferPool(sharedBufferPool, tableRef, l0s)
            db.trieCatalog.addTries(tableRef, l0s, Instant.now())
        }
    }


    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun singleL0Compaction(iteration: Int) {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0Trie = buildTrieDetails(docsTable.tableName, L0TrieKeys.first())
        val db = dbs[0]

        db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState).use {
            addL0s(docsTable, listOf(l0Trie))
            it.compactAllSync(null)
        }

        assertEquals(
            listOf("l01-rc-b00"),
            db.trieCatalog.listLiveAndNascentTrieKeys(docsTable),
        )
        assertEquals(
            setOf("l00-rc-b00"),
            db.trieCatalog.listAllGarbageTrieKeys(docsTable),
        )
    }

    @Test
    @WithSeed(-1748393987)
    fun singleL0CompactionNotHanging() {
        singleL0Compaction(0)
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun multipleL0ToL1Compaction(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val db = dbs[0]

        db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState).use {
            // Round 1: Add 3 L0 tries and compact
            addL0s(
                table,
                listOf(
                    buildTrieDetails(table.tableName, "l00-rc-b00", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b01", 10 * 1024),
                    buildTrieDetails(table.tableName, "l00-rc-b02", 10 * 1024)
                )
            )

            it.compactAllSync(null)

            // L1C is levelled — each undersized L1C is superseded by the next,
            // so only the latest (b02) remains live
            assertEquals(
                listOf("l01-rc-b02"),
                db.trieCatalog.listLiveAndNascentTrieKeys(table)
            )
            // 3 L0s garbage + 2 superseded L1Cs (b00, b01)
            assertEquals(
                setOf("l00-rc-b00", "l00-rc-b01", "l00-rc-b02", "l01-rc-b00", "l01-rc-b01"),
                db.trieCatalog.listAllGarbageTrieKeys(table)
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

            it.compactAllSync(null)

            // Only the latest L1C (b05) remains live after levelling
            assertEquals(
                listOf("l01-rc-b05"),
                db.trieCatalog.listLiveAndNascentTrieKeys(table)
            )
            // 6 L0s garbage + 5 superseded L1Cs (b00-b04)
            assertEquals(
                setOf(
                    "l00-rc-b00", "l00-rc-b01", "l00-rc-b02", "l00-rc-b03", "l00-rc-b04", "l00-rc-b05",
                    "l01-rc-b00", "l01-rc-b01", "l01-rc-b02", "l01-rc-b03", "l01-rc-b04",
                ),
                db.trieCatalog.listAllGarbageTrieKeys(table)
            )
        }
    }

    @Test
    @WithSeed(-1754611144)
    fun multipleL0ToL1CompactionIssue() {
        multipleL0ToL1Compaction(0)
    }

    @Test
    @WithCompactorDriverConfig(temporalSplitting = CURRENT)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun biggerCompactorRun() {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0tries = L0TrieKeys.take(100).map { buildTrieDetails(docsTable.tableName, it, 10L * 1024L * 1024L) }
        val db = dbs[0]

        db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState).use {
            addL0s(docsTable, l0tries.toList())
            it.compactAllSync(null)
            val liveTries = db.trieCatalog.listLiveAndNascentTrieKeys(docsTable)
            val garbageTries = db.trieCatalog.listAllGarbageTrieKeys(docsTable)
            assertEquals(0, liveTries.prefix("l00-rc-").size)
            assertTrue(garbageTries.any { it.startsWith("l00-rc-") }, "L0s should be in garbage")
            assertTrue(liveTries.prefix("l01-rc-").size > 0, "Some L1Cs should be live")
            assertTrue(liveTries.prefix("l02-rc-").size > 0, "Some L2Cs should be live")
        }
    }


    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @WithCompactorDriverConfig(temporalSplitting = CURRENT)
    fun l1cToL2cCompaction(iteration: Int) {
        val docsTable = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val db = dbs[0]

        val l1Tries = L1TrieKeys.take(4).map { buildTrieDetails(docsTable.tableName, it, defaultFileTarget) }

        db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState).use {
            addL0s(docsTable, l1Tries.toList())

            it.compactAllSync(null)

            assertEquals(
                setOf("l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03"),
                db.trieCatalog.listLiveAndNascentTrieKeys(docsTable).toSet()
            )
            assertEquals(
                setOf("l01-rc-b00", "l01-rc-b01", "l01-rc-b02", "l01-rc-b03"),
                db.trieCatalog.listAllGarbageTrieKeys(docsTable)
            )
        }
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @WithCompactorDriverConfig(temporalSplitting = CURRENT)
    fun l1cToL2cWithPartialL2(iteration: Int) {
        val docsTable = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val rand = Random(currentSeed)
        val db = dbs[0]

        // Create 4 full L1C tries
        val l1Tries = L1TrieKeys.take(4).map { buildTrieDetails(docsTable.tableName, it, defaultFileTarget) }

        // Randomly select which L2 partitions already exist (1-3 partitions)
        val numExistingPartitions = rand.nextInt(1, 4) // 1, 2, or 3
        val existingPartitions = (0..3).shuffled(rand).take(numExistingPartitions)

        val existingL2Tries = existingPartitions.map { partition ->
            buildTrieDetails(docsTable.tableName, "l02-rc-p$partition-b03", defaultFileTarget)
        }

        val missingPartitions = (0..3).filterNot { it in existingPartitions }

        db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState).use {
            addL0s(docsTable, l1Tries.toList() + existingL2Tries)

            it.compactAllSync(null)

            val liveTries = db.trieCatalog.listLiveAndNascentTrieKeys(docsTable)
            assertEquals(0, liveTries.prefix("l01-").size)
            assertEquals(
                setOf("l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03"),
                liveTries.prefix("l02-").toSet()
            )
            assertEquals(
                setOf("l01-rc-b00", "l01-rc-b01", "l01-rc-b02", "l01-rc-b03"),
                db.trieCatalog.listAllGarbageTrieKeys(docsTable)
            )
        }
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @WithCompactorDriverConfig(temporalSplitting = CURRENT)
    fun l2cGapFillingAndL3cCompaction(iteration: Int) {
        val docsTable = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val db = dbs[0]

        // Create a complex scenario with interleaved L1C and L2C tries
        // Simulates the "up to L3" test from compactor_test.clj
        val l1Tries = L1TrieKeys.take(16).map { buildTrieDetails(docsTable.tableName, it, defaultFileTarget) }
        val l2tries = mutableListOf<TrieDetails>()

        // Add some existing L2C partitions with gaps
        // Partition 0: has b03, b07, b0b (missing b0f to complete first L3)
        l2tries.add(buildTrieDetails(docsTable.tableName, "l02-rc-p0-b03", defaultFileTarget))
        l2tries.add(buildTrieDetails(docsTable.tableName, "l02-rc-p0-b07", defaultFileTarget))
        l2tries.add(buildTrieDetails(docsTable.tableName, "l02-rc-p0-b0b", defaultFileTarget))

        // Partition 1: completely missing (will be created from L1s)

        // Partition 2: has b03, b07 (missing b0b, b0f)
        l2tries.add(buildTrieDetails(docsTable.tableName, "l02-rc-p2-b03", defaultFileTarget))
        l2tries.add(buildTrieDetails(docsTable.tableName, "l02-rc-p2-b07", defaultFileTarget))

        // Partition 3: has b03, b07 (missing b0b, b0f)
        l2tries.add(buildTrieDetails(docsTable.tableName, "l02-rc-p3-b03", defaultFileTarget))
        l2tries.add(buildTrieDetails(docsTable.tableName, "l02-rc-p3-b07", defaultFileTarget))

        db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState).use {
            addL0s(docsTable, l1Tries.toList() + l2tries)

            it.compactAllSync(null)

            val liveTries = db.trieCatalog.listLiveAndNascentTrieKeys(docsTable)

            // L1Cs should all be garbage after L2C supersession
            assertEquals(0, liveTries.prefix("l01-rc").size)

            // L2Cs should all be garbage after L3C supersession
            assertEquals(0, liveTries.prefix("l02-rc").size)

            // L3Cs should be live
            assertEquals(16, liveTries.prefix("l03-rc").size)
        }
    }

    private fun runConcurrentTableCompaction() {
        val docsTable = TableRef("xtdb", "public", "docs")
        val usersTable = TableRef("xtdb", "public", "users")
        val ordersTable = TableRef("xtdb", "public", "orders")
        val l0FileSize = 100L * 1024L * 1024L

        // Start from L0 files to test the full compaction pipeline with multiple tables
        val docsTries = L0TrieKeys.take(16).map { buildTrieDetails(docsTable.tableName, it, l0FileSize) }
        val usersTries = L0TrieKeys.take(16).map { buildTrieDetails(usersTable.tableName, it, l0FileSize) }
        val ordersTries = L0TrieKeys.take(16).map { buildTrieDetails(ordersTable.tableName, it, l0FileSize) }

        addL0s(docsTable, docsTries.toList())
        addL0s(usersTable, usersTries.toList())
        addL0s(ordersTable, ordersTries.toList())

        dbs.safeMap { db ->
            db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState)
        }.useAll { compactorsForDb ->
            runBlocking {
                compactorsForDb.shuffled(rand).map { c -> async { c.compactAll() } }.awaitAll()
            }
        }

        // Verify all catalogs have the same results
        dbs.forEach { db ->
            val docsKeys = db.trieCatalog.listLiveAndNascentTrieKeys(docsTable)
            val usersKeys = db.trieCatalog.listLiveAndNascentTrieKeys(usersTable)
            val ordersKeys = db.trieCatalog.listLiveAndNascentTrieKeys(ordersTable)
            assertEquals(0, docsKeys.prefix("l00-rc-").size)
            assertEquals(0, docsKeys.prefix("l01-rc-").size)
            assertEquals(0, docsKeys.prefix("l02-rc-").size)
            assertEquals(16, docsKeys.prefix("l03-rc-").size)
            // 16 L0 + 16 L1C + 16 L2C all superseded
            assertEquals(48, db.trieCatalog.listAllGarbageTrieKeys(docsTable).size)
            assertEquals(docsKeys.toSet(), usersKeys.toSet(), "Docs and Users tables should have identical trie keys")
            assertEquals(docsKeys.toSet(), ordersKeys.toSet(), "Docs and Orders tables should have identical trie keys")
        }

    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @WithCompactorDriverConfig(temporalSplitting = CURRENT)
    fun concurrentTableCompaction(iteration: Int) {
        runConcurrentTableCompaction()
    }

    @RepeatableSimulationTest
    @WithNumberOfSystems(2)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @WithCompactorDriverConfig(temporalSplitting = CURRENT)
    fun multiSystemConcurrentTableCompaction(iteration: Int) {
        runConcurrentTableCompaction()
    }

    @RepeatableSimulationTest
    @WithNumberOfSystems(2)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun multiSystemSingleL0Compaction(iteration: Int) {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0Trie = buildTrieDetails(docsTable.tableName, L0TrieKeys.first())

        addL0s(docsTable, listOf(l0Trie))

        dbs.safeMap { db ->
            db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState)
        }.useAll { compactorsForDb ->
            runBlocking {
                compactorsForDb.shuffled(rand).map { c -> async { c.compactAll() } }.awaitAll()
            }
        }

        val trieKeys = dbs.map { it.trieCatalog.listLiveAndNascentTrieKeys(docsTable) }.distinct()

        assertEquals(
            1,
            trieKeys.size,
        )
        assertEquals(
            listOf("l01-rc-b00"),
            trieKeys.first(),
        )
        dbs.forEach { db ->
            assertEquals(setOf("l00-rc-b00"), db.trieCatalog.listAllGarbageTrieKeys(docsTable))
        }
    }

    @RepeatedTest(10)
    @WithNumberOfSystems(2)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @WithCompactorDriverConfig(temporalSplitting = BOTH)
    fun biggerMultiSystemCompactorRun() {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0tries = L0TrieKeys.take(1000).map { buildTrieDetails(docsTable.tableName, it, 10L * 1024L * 1024L) }

        addL0s(docsTable, l0tries.toList())

        dbs.safeMap { db ->
            db.compactor.openForDatabase(allocator, db.dbStorage, db.dbState)
        }.useAll { compactorsForDb ->
            runBlocking {
                compactorsForDb.shuffled(rand).map { c -> async { c.compactAll() } }.awaitAll()
            }
        }

        val trieKeys = dbs.map { it.trieCatalog.listLiveAndNascentTrieKeys(docsTable) }

        assertEquals(
            1,
            trieKeys.map { it.size }.distinct().size,
        )
    }
}

