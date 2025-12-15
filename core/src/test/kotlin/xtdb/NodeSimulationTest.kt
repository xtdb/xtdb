package xtdb

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import xtdb.SimulationTestUtils.Companion.L1TrieKeys
import xtdb.SimulationTestUtils.Companion.addTriesToBufferPool
import xtdb.SimulationTestUtils.Companion.buildTrieDetails
import xtdb.SimulationTestUtils.Companion.createJobCalculator
import xtdb.SimulationTestUtils.Companion.createTrieCatalog
import xtdb.SimulationTestUtils.Companion.prefix
import xtdb.SimulationTestUtils.Companion.setLogLevel
import xtdb.api.TransactionKey
import xtdb.api.log.Log
import xtdb.arrow.unsupported
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.compactor.CompactorDriverConfig
import xtdb.compactor.CompactorMockDriver
import xtdb.compactor.RepeatableSimulationTest
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import xtdb.SimulationTestUtils.Companion.L0TrieKeys
import xtdb.SimulationTestUtils.Companion.L3TrieKeys
import xtdb.compactor.MockDb
import xtdb.database.DatabaseName
import xtdb.database.IDatabase
import xtdb.garbage_collector.GarbageCollector
import xtdb.garbage_collector.GarbageCollectorMockDriver
import xtdb.indexer.Indexer
import xtdb.indexer.LogProcessor
import xtdb.log.proto.TrieDetails
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool
import xtdb.storage.MemoryStorage
import xtdb.table.TableRef
import xtdb.trie.TrieCatalog
import xtdb.util.asPath
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.UUID
import kotlin.time.Duration.Companion.microseconds

data class MockDatabase(
    override val name: DatabaseName,
    override val allocator: BufferAllocator,
    override val bufferPool: BufferPool,
    override val trieCatalog: TrieCatalog,
    override val blockCatalog: BlockCatalog,
    private val compactorOrNull: Compactor.ForDatabase?,
) : IDatabase {
    override val compactor: Compactor.ForDatabase get() = compactorOrNull ?: error("compactor not initialised")
    override val tableCatalog: TableCatalog get() = unsupported("tableCatalog")
    override val log: Log get() = unsupported("log")
    override val metadataManager: PageMetadata.Factory get() = unsupported("metadataManager")
    override val logProcessor: LogProcessor get() = unsupported("logProcessor")
    override val txSink: Indexer.TxSink get() = unsupported("txSink")
    fun withCompactor(compactor: Compactor.ForDatabase) = copy(compactorOrNull = compactor)
}

fun listTrieNamesFromBufferPool(bufferPool: BufferPool, tableRef: TableRef): List<String> =
    bufferPool
        .listAllObjects("tables/public\$${tableRef.tableName}/data/".asPath)
        .map { it.key.fileName.toString().removeSuffix(".arrow") }

@ParameterizedTest(name = "[iteration {0}]")
@MethodSource("xtdb.SimulationTestBase#iterationSource")
annotation class RepeatableSimulationTest

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithNumberOfSystems(val numberOfSystems: Int)

class NumberOfSystemsExtension : BeforeEachCallback {
    override fun beforeEach(context: ExtensionContext) {
        val annotation = context.requiredTestMethod.getAnnotation(WithNumberOfSystems::class.java)
            ?: return

        val testInstance = context.requiredTestInstance
        if (testInstance !is NodeSimulationTest) return

        testInstance.numberOfSystems = annotation.numberOfSystems
    }
}

// Settings used by all tests in this class
private const val logLevel = "WARN"

@Tag("property")
@ExtendWith(NumberOfSystemsExtension::class)
class NodeSimulationTest : SimulationTestBase() {
    var numberOfSystems: Int = 1
    val garbageLifetime = Duration.ofSeconds(60)
    private lateinit var allocator: BufferAllocator
    private lateinit var sharedBufferPool: MemoryStorage
    private lateinit var compactorDriver: CompactorMockDriver
    private lateinit var gcDriver: GarbageCollectorMockDriver
    private lateinit var compactors: List<Compactor.Impl>
    private lateinit var compactorsForDb: List<Compactor.ForDatabase>
    private lateinit var blockCatalogs: List<BlockCatalog>
    private lateinit var trieCatalogs: List<TrieCatalog>
    private lateinit var dbs: List<MockDatabase>
    private lateinit var garbageCollectors: List<GarbageCollector>

    @BeforeEach
    fun setUp() {
        super.setUpSimulation()
        setLogLevel.invoke("xtdb".symbol, logLevel)

        val jobCalculator = createJobCalculator.invoke() as Compactor.JobCalculator
        compactorDriver = CompactorMockDriver(dispatcher, currentSeed, CompactorDriverConfig())
        gcDriver = GarbageCollectorMockDriver()
        allocator = RootAllocator()

        // Create a single shared buffer pool for all systems
        sharedBufferPool = MemoryStorage(allocator, epoch = 0)

        compactors = List(numberOfSystems) {
            Compactor.Impl(compactorDriver, null, jobCalculator, false, 2, dispatcher)
        }
        trieCatalogs = List(numberOfSystems) {
            createTrieCatalog.invoke(null, null, mutableMapOf<Any, Any>(), 100 * 1024 * 1024) as TrieCatalog
        }
        blockCatalogs = List(numberOfSystems) { i ->
            BlockCatalog("xtdb", sharedBufferPool)
        }
        val uninitializedDbs = List(numberOfSystems) { i ->
            MockDatabase("xtdb", allocator, sharedBufferPool, trieCatalogs[i], blockCatalogs[i], null)
        }
        compactorsForDb = uninitializedDbs.mapIndexed {
            i, db -> compactors[i].openForDatabase(db)
        }
        dbs = uninitializedDbs.mapIndexed {
            i, db -> db.withCompactor(compactorsForDb[i])
        }
        garbageCollectors = dbs.map {
            db -> GarbageCollector.Impl(db, gcDriver, 2, garbageLifetime, Duration.ofSeconds(30), dispatcher)
        }
    }

    @AfterEach
    fun tearDown() {
        super.tearDownSimulation()
        garbageCollectors.forEach { it.close() }
        sharedBufferPool.close()
        compactorsForDb.forEach { it.close() }
        compactors.forEach { it.close() }
        allocator.close()
    }

    private fun addTries(tableRef: TableRef, tries: List<TrieDetails>, timestamp: Instant) {
        tries.forEach {
            compactorDriver.trieKeyToFileSize[it.trieKey.toString()] = it.dataFileSize
        }
        dbs.forEach { db ->
            db.trieCatalog.addTries(tableRef, tries, timestamp)
            addTriesToBufferPool(db.bufferPool, tableRef, tries)
        }
    }


    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `l1 compaction followed by garbage collection`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val l1Tries = L1TrieKeys.take(4).toList()
        val blockCatalog = blockCatalogs[0]
        val trieCatalog = trieCatalogs[0]
        val compactorForDb = compactorsForDb[0]
        val garbageCollector = garbageCollectors[0]

        addTries(
            table,
            l1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        for (blockIndex in 1L..3L) {
            blockCatalog.finishBlock(
                blockIndex = blockIndex,
                latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                latestProcessedMsgId = blockIndex,
                tables = listOf(table),
                secondaryDatabases = null
            )
        }

        Assertions.assertEquals(l1Tries, trieCatalog.listAllTrieKeys(table), "l1s present in trie catalog")
        Assertions.assertEquals(l1Tries, listTrieNamesFromBufferPool(sharedBufferPool, table), "l1s present in buffer pool")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }

        Assertions.assertEquals(l1Tries, trieCatalog.listAllTrieKeys(table), "live l1s haven't been garbage collected")
        Assertions.assertEquals(l1Tries, listTrieNamesFromBufferPool(sharedBufferPool, table), "live l1s haven't been garbage collected")

        compactorForDb.compactAll()

        val l2Tries = listOf("l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03")
        val allTries = (l1Tries + l2Tries).toSet()

        Assertions.assertEquals(allTries, listTrieNamesFromBufferPool(sharedBufferPool, table).toSet(), "l2s present in buffer pool")
        Assertions.assertEquals(allTries, trieCatalog.listAllTrieKeys(table).toSet(), "l2s present in trie catalog")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }

        Assertions.assertEquals(l2Tries.toSet(), trieCatalog.listAllTrieKeys(table).toSet(), "l1s should get garbage collected from trie catalog")
        Assertions.assertEquals(l2Tries.toSet(), listTrieNamesFromBufferPool(sharedBufferPool, table).toSet(), "l1s should get garbage collected from buffer pool")
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `gc during compaction preserves files`(iteration: Int)  {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val l1Tries = L1TrieKeys.take(8).toList()
        val blockCatalog = blockCatalogs[0]
        val trieCatalog = trieCatalogs[0]
        val compactorForDb = compactorsForDb[0]
        val garbageCollector = garbageCollectors[0]
        val expectedL2Tries = listOf(
            "l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03",
            "l02-rc-p0-b07", "l02-rc-p1-b07", "l02-rc-p2-b07", "l02-rc-p3-b07"
        )

        addTries(
            table,
            l1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Finish blocks so GC can consider tries for collection
        for (blockIndex in 5L..7L) {
            blockCatalog.finishBlock(
                blockIndex = blockIndex,
                latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                latestProcessedMsgId = blockIndex,
                tables = listOf(table),
                secondaryDatabases = null
            )
        }

        Assertions.assertEquals(l1Tries, trieCatalog.listAllTrieKeys(table), "l1s present initially")
        Assertions.assertEquals(l1Tries, listTrieNamesFromBufferPool(sharedBufferPool, table), "l1s present in buffer pool initially")

        // Launch both compaction and GC on the dispatcher using coroutines
        runBlocking(dispatcher) {
            val compactionJob = launch {
                compactorForDb.startCompaction().await()
            }
            val gcJob = launch {
                val asOf = Instant.now() + Duration.ofHours(1)
                while(trieCatalog.garbageTries(table, asOf).isEmpty()) { yield() } // wait for compaction to mark tries as garbage
                garbageCollector.garbageCollectTries(asOf)
            }

            compactionJob.join()
            gcJob.join()
        }

        val triesInCatalog = trieCatalog.listAllTrieKeys(table)
        val triesInBufferPool = listTrieNamesFromBufferPool(sharedBufferPool, table)

        Assertions.assertTrue(expectedL2Tries.all { it in triesInCatalog }, "All L2 tries should be present in catalog.")
        Assertions.assertTrue(expectedL2Tries.all { it in triesInBufferPool }, "All L2 tries should be present in buffer pool.")

        val l1sInCatalog = triesInCatalog.prefix("l01-rc-")
        Assertions.assertTrue(l1sInCatalog.size < 8, "Some l1s should get garbage collected from trie catalog mid compaction.")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }

        Assertions.assertEquals(expectedL2Tries.toSet(), trieCatalog.listAllTrieKeys(table).toSet(), "l1s should get garbage collected from trie catalog, leaving l2s")
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `gc collects old garbage while compaction runs`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val blockCatalog = blockCatalogs[0]
        val trieCatalog = trieCatalogs[0]
        val compactorForDb = compactorsForDb[0]
        val garbageCollector = garbageCollectors[0]

        val oldl1Tries = L1TrieKeys.take(4).toList()
        val oldl2Tries = listOf("l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03")
        val oldTries = oldl1Tries + oldl2Tries

        addTries(
            table,
            oldTries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        Assertions.assertEquals(oldTries, trieCatalog.listAllTrieKeys(table), "tries present in catalog")
        Assertions.assertEquals(oldTries, listTrieNamesFromBufferPool(sharedBufferPool, table), "tries present in buffer pool")

        val newL1Tries = listOf("l01-rc-b04", "l01-rc-b05", "l01-rc-b06", "l01-rc-b07")
        addTries(
            table,
            newL1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Finish blocks so GC can consider tries for collection
        for (blockIndex in 5L..7L) {
            blockCatalog.finishBlock(
                blockIndex = blockIndex,
                latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                latestProcessedMsgId = blockIndex,
                tables = listOf(table),
                secondaryDatabases = null
            )
        }

        val expectedNewL2Tries = listOf("l02-rc-p0-b07", "l02-rc-p1-b07", "l02-rc-p2-b07", "l02-rc-p3-b07")

        // Launch compaction (for new L1s) and GC (for old garbage L1s) in parallel
        runBlocking(dispatcher) {
            val compactionJob = launch {
                compactorForDb.startCompaction().await()
            }
            val gcJob = launch {
                garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }

            compactionJob.join()
            gcJob.join()
        }

        val finalTriesInCatalog = trieCatalog.listAllTrieKeys(table).toSet()
        val finalTriesInBufferPool = listTrieNamesFromBufferPool(sharedBufferPool, table).toSet()

        Assertions.assertTrue(oldl1Tries.none { it in finalTriesInCatalog }, "Old L1 tries should be garbage collected")
        Assertions.assertTrue(oldl1Tries.none { it in finalTriesInBufferPool }, "Old L1 tries should be removed from buffer pool")

        Assertions.assertTrue(expectedNewL2Tries.all { it in finalTriesInCatalog }, "New L2 tries should be created by compaction")
        Assertions.assertTrue(expectedNewL2Tries.all { it in finalTriesInBufferPool }, "New L2 tries should be in buffer pool")

        Assertions.assertTrue(oldl2Tries.all { it in finalTriesInCatalog }, "Old L2 tries should still be present")
        Assertions.assertTrue(oldl2Tries.all { it in finalTriesInBufferPool }, "Old L2 tries should still be in buffer pool")

        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }
        val l2Set = (oldl2Tries + expectedNewL2Tries).toSet()
        Assertions.assertEquals(l2Set.toSet(), trieCatalog.listAllTrieKeys(table).toSet(), "l1s should get garbage collected from trie catalog, leaving l2s")
    }

    @RepeatableSimulationTest
    @WithNumberOfSystems(2)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `multi-system compaction and gc`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L

        // Add some old L1 tries and their corresponding L2s (already compacted, so L1s are garbage)
        val oldL1Tries = L1TrieKeys.take(4).toList()
        val oldL2Tries = listOf("l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03")
        addTries(
            table,
            (oldL1Tries + oldL2Tries).map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Add new L1 tries that will be compacted
        val newL1Tries = listOf("l01-rc-b04", "l01-rc-b05", "l01-rc-b06", "l01-rc-b07")
        addTries(
            table,
            newL1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Finish blocks so GC can consider tries for collection
        blockCatalogs.forEach { blockCatalog ->
            for (blockIndex in 5L..7L) {
                blockCatalog.finishBlock(
                    blockIndex = blockIndex,
                    latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                    latestProcessedMsgId = blockIndex,
                    tables = listOf(table),
                    secondaryDatabases = null
                )
            }
        }

        val expectedNewL2Tries = listOf("l02-rc-p0-b07", "l02-rc-p1-b07", "l02-rc-p2-b07", "l02-rc-p3-b07")
        val expectedL2Tries = oldL2Tries + expectedNewL2Tries

        // Launch compaction and GC across all systems in parallel
        runBlocking(dispatcher) {
            val compactionJobs = compactorsForDb.shuffled(rand).map { compactor ->
                launch {
                    compactor.startCompaction().await()
                }
            }
            val gcJobs = garbageCollectors.shuffled(rand).map { gc ->
                val jobId = UUID.randomUUID().toString().substring(0, 8)
                launch(CoroutineName("gc-$jobId")) {
                    gc.garbageCollectTries(Instant.now() + Duration.ofHours(1))
                }
            }

            (compactionJobs + gcJobs).joinAll()
        }

        // Verify all catalogs have the expected L2 tries and some old L1s were GCed
        trieCatalogs.forEach { trieCatalog ->
            val triesInCatalog = trieCatalog.listAllTrieKeys(table).toSet()
            Assertions.assertTrue(expectedL2Tries.all { it in triesInCatalog }, "All L2 tries should be present in all catalogs")

            val oldL1sRemaining = oldL1Tries.count { it in triesInCatalog }
            Assertions.assertTrue(oldL1sRemaining < oldL1Tries.size, "Some old L1s should have been garbage collected")
        }

        // Final GC pass to clean up remaining L1s
        runBlocking {
            garbageCollectors.forEach { gc ->
                gc.garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }
        }

        // Verify all systems converged to the same state
        val allTrieSets = trieCatalogs.map { it.listAllTrieKeys(table).toSet() }
        Assertions.assertEquals(1, allTrieSets.distinct().size, "All systems should have converged to the same trie set")
        Assertions.assertEquals(expectedL2Tries.toSet(), allTrieSets.first(), "Final state should only contain L2 tries")
    }

    @RepeatableSimulationTest
    @WithNumberOfSystems(2)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `staggered system startup during active compaction`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L

        // Add L1 tries that will be compacted
        val l1Tries = L1TrieKeys.take(8).toList()
        addTries(
            table,
            l1Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Finish blocks so compaction can proceed
        blockCatalogs.forEach { blockCatalog ->
            for (blockIndex in 5L..7L) {
                blockCatalog.finishBlock(
                    blockIndex = blockIndex,
                    latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                    latestProcessedMsgId = blockIndex,
                    tables = listOf(table),
                    secondaryDatabases = null
                )
            }
        }

        val expectedL2Tries = listOf(
            "l02-rc-p0-b03", "l02-rc-p1-b03", "l02-rc-p2-b03", "l02-rc-p3-b03",
            "l02-rc-p0-b07", "l02-rc-p1-b07", "l02-rc-p2-b07", "l02-rc-p3-b07"
        )

        // System 0: Start compaction and GC immediately
        // System 1: Start compaction and GC AFTER system 0 has already finished
        runBlocking(dispatcher) {
            // System 0 runs compaction and GC to completion
            val system0Compaction = launch(CoroutineName("system0-compaction")) {
                compactorsForDb[0].startCompaction().await()
            }
            val system0GC = launch(CoroutineName("system0-gc")) {
                garbageCollectors[0].garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }

            system0Compaction.join()
            system0GC.join()

            // Verify system 0 has completed its work
            val system0Tries = trieCatalogs[0].listAllTrieKeys(table).toSet()
            Assertions.assertTrue(
                expectedL2Tries.all { it in system0Tries },
                "System 0 should have all L2 tries after compaction"
            )

            // Now system 1 starts its compaction and GC
            val system1Compaction = launch(CoroutineName("system1-compaction")) {
                compactorsForDb[1].startCompaction().await()
            }
            val system1GC = launch(CoroutineName("system1-gc")) {
                garbageCollectors[1].garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }

            system1Compaction.join()
            system1GC.join()
        }

        // Verify both systems have the expected L2 tries after all operations
        trieCatalogs.forEachIndexed { index, trieCatalog ->
            val triesInCatalog = trieCatalog.listAllTrieKeys(table).toSet()
            Assertions.assertTrue(
                expectedL2Tries.all { it in triesInCatalog },
                "System $index should have all L2 tries present"
            )
        }

        // Final GC pass
        runBlocking {
            garbageCollectors.forEach { gc ->
                gc.garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }
        }

        val allTrieSets = trieCatalogs.map { it.listAllTrieKeys(table).toSet() }
        Assertions.assertEquals(1, allTrieSets.distinct().size, "All systems should converge to the same trie set despite staggered startup")
        Assertions.assertEquals(expectedL2Tries.toSet(), allTrieSets.first(), "Final state should only contain L2 tries, no L1s should remain")

        val bufferPoolTries = listTrieNamesFromBufferPool(sharedBufferPool, table).toSet()
        Assertions.assertEquals(expectedL2Tries.toSet(), bufferPoolTries, "Buffer pool should only contain L2 tries")
    }

    @RepeatableSimulationTest
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `l0 to l3 compaction and gc`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L
        val blockCatalog = blockCatalogs[0]
        val trieCatalog = trieCatalogs[0]
        val compactorForDb = compactorsForDb[0]
        val garbageCollector = garbageCollectors[0]

        val l0Tries = L0TrieKeys.take(16).toList()

        addTries(
            table,
            l0Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        for (blockIndex in 1L..15L) {
            blockCatalog.finishBlock(
                blockIndex = blockIndex,
                latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                latestProcessedMsgId = blockIndex,
                tables = listOf(table),
                secondaryDatabases = null
            )
        }

        compactorForDb.compactAll()
        runBlocking { garbageCollector.garbageCollectTries(Instant.now() + Duration.ofHours(1)) }

        val triesInCatalog = trieCatalog.listAllTrieKeys(table)
        val l0Count = triesInCatalog.prefix("l00-rc-").size
        val l1Count = triesInCatalog.prefix("l01-rc-").size
        val l2Count = triesInCatalog.prefix("l02-rc-").size
        val l3Count = triesInCatalog.prefix("l03-rc-").size
        Assertions.assertEquals(16, l0Count, "L0 tries should still be present in catalog")
        Assertions.assertEquals(0, l1Count, "L1 tries should be fully compacted and garbage collected")
        Assertions.assertEquals(0, l2Count, "L2 tries should be fully compacted and garbage collected")
        Assertions.assertEquals(16, l3Count, "Should have at least 16 L3 tries after cascading compaction")

        val triesInBufferPool = listTrieNamesFromBufferPool(sharedBufferPool, table)
        Assertions.assertEquals(triesInCatalog.toSet(), triesInBufferPool.toSet(), "Buffer pool should match catalog after compaction and GC")

    }

    @RepeatableSimulationTest
    @WithNumberOfSystems(2)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `l0 to 13 with concurrent compaction + gc`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L

        val l0Tries = L0TrieKeys.take(16).toList()

        addTries(
            table,
            l0Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        // Finish blocks to enable compaction
        blockCatalogs.forEach { blockCatalog ->
            for (blockIndex in 1L..15L) {
                blockCatalog.finishBlock(
                    blockIndex = blockIndex,
                    latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                    latestProcessedMsgId = blockIndex,
                    tables = listOf(table),
                    secondaryDatabases = null
                )
            }
        }

        // Launch compaction (L0→L1→L2→L3) and GC across all systems concurrently
        runBlocking(dispatcher) {
            val compactionJobs = compactorsForDb.shuffled(rand).map { compactor ->
                launch {
                    compactor.startCompaction().await()
                }
            }

            val gcJobs = garbageCollectors.shuffled(rand).map { gc ->
                launch(CoroutineName("gc")) {
                    gc.garbageCollectTries(Instant.now() + Duration.ofHours(1))
                }
            }

            (compactionJobs + gcJobs).joinAll()
        }

        trieCatalogs.forEach { trieCatalog ->
            val triesInCatalog = trieCatalog.listAllTrieKeys(table)
            val l0Count = triesInCatalog.prefix("l00-rc-").size
            val l3Count = triesInCatalog.prefix("l03-rc-").size

            Assertions.assertEquals(16, l0Count, "L0 tries should still be present in catalog")
            Assertions.assertEquals(16, l3Count, "Should have at least 16 L3 tries after cascading compaction")
        }

        // Final GC pass
        runBlocking {
            garbageCollectors.forEach { gc ->
                gc.garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }
        }

        // Verify all systems converged
        val allTrieSets = trieCatalogs.map { it.listAllTrieKeys(table).toSet() }
        Assertions.assertEquals(1, allTrieSets.distinct().size, "All systems should converge to the same trie set")

        val finalTries = allTrieSets.first()
        val l0Count = finalTries.count { it.startsWith("l00-rc-") }
        val l1Count = finalTries.count { it.startsWith("l01-rc-") }
        val l2Count = finalTries.count { it.startsWith("l02-rc-") }
        val l3Count = finalTries.count { it.startsWith("l03-rc-") }

        Assertions.assertEquals(16, l0Count, "L0 tries are never marked as garbage")
        Assertions.assertEquals(0, l1Count, "L1 tries should be fully GCed after final pass")
        Assertions.assertEquals(0, l2Count, "L2 tries should be fully GCed after final pass")
        Assertions.assertEquals(16, l3Count, "Should have at 16 L3 tries after final GC")

        // Verify buffer pool matches catalog
        val bufferPoolTries = listTrieNamesFromBufferPool(sharedBufferPool, table).toSet()
        Assertions.assertEquals(finalTries.size, bufferPoolTries.size, "Buffer pool trie count should match catalog")
        Assertions.assertEquals(finalTries, bufferPoolTries, "Buffer pool should match catalog")
    }

    @RepeatableSimulationTest
    @WithNumberOfSystems(2)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `multi system l3 to l4 compaction`(iteration: Int) {
        val table = TableRef("xtdb", "public", "docs")
        val defaultFileTarget = 100L * 1024L * 1024L

        // Using helper to generate 52 l3 tries
        // - this generates p00->p03 for each block
        // - we expect 13 blocks
        // - therefore, we expect 48 l4 tries after compaction, and 4 l3 tries remaining/that couldn't be GCed
        val l3Tries = L3TrieKeys.take(52).toList()

        addTries(
            table,
            l3Tries.map { buildTrieDetails(table.tableName, it, defaultFileTarget) },
            Instant.now()
        )

        blockCatalogs.forEach { blockCatalog ->
            for (blockIndex in 0L..13L) {
                blockCatalog.finishBlock(
                    blockIndex = blockIndex,
                    latestCompletedTx = TransactionKey(txId = blockIndex, systemTime = Instant.now()),
                    latestProcessedMsgId = blockIndex,
                    tables = listOf(table),
                    secondaryDatabases = null
                )
            }
        }

        // Run compaction and GC concurrently
        runBlocking(dispatcher) {
            val compactionJobs = compactorsForDb.shuffled(rand).map { compactor ->
                launch {
                    compactor.startCompaction().await()
                }
            }

            val gcJobs = garbageCollectors.shuffled(rand).flatMap { gc ->
                // Run multiple GC rounds at different times to catch tries at various stages
                List(3) { round ->
                    launch(CoroutineName("gc-round$round")) {
                        repeat(round) { yield() }
                        gc.garbageCollectTries(Instant.now() + Duration.ofHours(1))
                    }
                }
            }

            (compactionJobs + gcJobs).joinAll()
        }

        trieCatalogs.first().let { trieCatalog ->
            val triesInCatalog = trieCatalog.listAllTrieKeys(table)
            val l4Count = triesInCatalog.prefix("l04-rc-").size
            Assertions.assertEquals(48, l4Count, "Should have 16 L4 tries (from 4 complete L3 sets)")
        }

        // Final GC pass to clean up all remaining garbage
        runBlocking {
            garbageCollectors.forEach { gc ->
                gc.garbageCollectTries(Instant.now() + Duration.ofHours(1))
            }
        }

        // Verify all systems converged
        val allTrieSets = trieCatalogs.map { it.listAllTrieKeys(table).toSet() }
        Assertions.assertEquals(1, allTrieSets.distinct().size, "All systems should converge to the same trie set")

        // Validate final counts - we should have L3 and L4 tries
        val finalTries = allTrieSets.first()
        val expectedL3s = setOf("l03-rc-p00-b0c", "l03-rc-p01-b0c", "l03-rc-p02-b0c", "l03-rc-p03-b0c")
        val finalL3s = finalTries.filter { it.startsWith("l03-rc-") }
        val l4Count = finalTries.count { it.startsWith("l04-rc-") }
        Assertions.assertEquals(expectedL3s, finalL3s.toSet(), "Should have 4 remaining L3 tries")
        Assertions.assertEquals(48, l4Count, "Should have 16 L4 tries after final GC")

        // Verify buffer pool matches catalog
        val bufferPoolTries = listTrieNamesFromBufferPool(sharedBufferPool, table).toSet()
        Assertions.assertEquals(finalTries.size, bufferPoolTries.size, "Buffer pool trie count should match catalog")
        Assertions.assertEquals(finalTries, bufferPoolTries, "Buffer pool should match catalog")
    }
}