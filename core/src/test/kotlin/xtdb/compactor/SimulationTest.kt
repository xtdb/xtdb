package xtdb.compactor

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestWatcher
import java.util.concurrent.TimeUnit
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
import xtdb.util.trace
import xtdb.util.warn
import java.time.Instant
import java.util.concurrent.LinkedBlockingQueue
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

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

class MockDriver(seed: Int = 0) : Factory {

    sealed interface AsyncMessage

    class AppendMessage(val msg: TriesAdded, val msgTimestamp: Instant) : AsyncMessage
    class AwaitSignalMessage(val cont: Continuation<JobKey?>) : AsyncMessage
    class Launch(val f: suspend () -> Unit) : AsyncMessage

    val outerRand = Random(seed)

    override fun create(scope: CoroutineScope, db: IDatabase) = ForDatabase(scope, db)

    inner class ForDatabase(scope: CoroutineScope, private val db: IDatabase) : Driver {
        val rand = Random(outerRand.nextInt())

        val channel = Channel<AsyncMessage>(UNLIMITED)

        var started = false
        var awaitSignalMessage: AwaitSignalMessage? = null

        var wokenUp: Boolean = false

        val launchedJobs = mutableSetOf<suspend () -> Unit>()
        val doneJobs = LinkedBlockingQueue<JobKey>()

        val logMessages = LinkedBlockingQueue<AppendMessage>()
        val trieCat = db.trieCatalog

        private fun consumeAll() {
            while (true) {
                channel.tryReceive()
                    .onSuccess { msg ->
                        started = true
                        LOGGER.trace("Msg received: $msg")
                        when (msg) {
                            is AppendMessage -> logMessages.add(msg)

                            is AwaitSignalMessage -> {

                                check(awaitSignalMessage == null)
                                awaitSignalMessage = msg
                            }

                            is Launch -> launchedJobs.add(msg.f)
                        }

                    }
                    .onFailure { return } // channel is empty
                    .onClosed { throw (it ?: CancellationException()) }
            }
        }

        private fun handleAwaitSignal(message: AwaitSignalMessage) : Boolean {
            when {
                doneJobs.isNotEmpty<JobKey>() && rand.nextDouble() < 0.8 -> {
                    val doneJob = doneJobs.poll()!!

                    message.cont.resume(doneJob)
                    awaitSignalMessage = null
                    return true
                }

                wokenUp && rand.nextDouble() < 0.8 -> {
                    message.cont.resume(null)
                    awaitSignalMessage = null
                    wokenUp = false
                    return true
                }
            }
            return false
        }

        init {
            scope.launch {
                try {
                    while (true) {
                        yield()
                        consumeAll()

                        val logs = mutableListOf<AppendMessage>()
                        logMessages.drainTo(logs)

                        logs.forEach { logMsg ->
                            logMsg.msg.tries
                                .groupBy { it.tableName }
                                .forEach { (tableName, tries) ->
                                    LOGGER.trace("Adding tries to TrieCatalog for table $tableName: $tries")
                                    trieCat.addTries(TableRef.parse(db.name, tableName), tries, logMsg.msgTimestamp)
                                }
                        }

                        awaitSignalMessage?.let { if(handleAwaitSignal(it)) continue }

                        launchedJobs.randomOrNull(rand)?.let { launchedJob ->
                            LOGGER.trace("Launching job...")
                            launchedJobs.remove(launchedJob)
                            launchedJob()
                            continue
                        }

                        if (started) break else delay(10.milliseconds)
                    }
                    close(null)
                } catch (e: Throwable) {
                    close(e)
                    throw e
                }
            }
        }

        override suspend fun launchIn(jobsScope: CoroutineScope, f: suspend () -> Unit) =
            channel.send(Launch(f))

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

        override suspend fun awaitSignal(): JobKey? = suspendCoroutine { cont ->
            channel.trySendBlocking(AwaitSignalMessage(cont)).exceptionOrNull()?.let{ cont.resumeWithException(CancellationException()) }
        }

        override suspend fun jobDone(jobKey: JobKey) {
            doneJobs.add(jobKey)
        }

        override fun wakeup() {
            wokenUp = true
        }

        override fun close() = close(null)

        fun close(e: Throwable?) {
            channel.close(e)
            consumeAll()
            awaitSignalMessage?.let { msg -> msg.cont.resumeWithException(e ?: CancellationException()); awaitSignalMessage = null }
        }
    }
}

private fun buildTrieDetails(tableName: String, trieKey: String): TrieDetails =
    TrieDetails.newBuilder()
        .setTableName(tableName)
        .setTrieKey(trieKey)
        .setDataFileSize(1024L)
        .build()

class SeedLogger : TestWatcher {
    override fun testFailed(context: ExtensionContext, cause: Throwable) {
        val testInstance = context.testInstance.orElse(null)
        if (testInstance is SimulationTest) {
            LOGGER.warn(cause, "Test failed with seed: ${testInstance.currentSeed}",)
        }
    }
}

// Settings used by all tests in this class
private const val logLevel = "TRACE"
private const val testIterations = 10

// Clojure interop to get at internal functions
private val setLogLevel = requiringResolve("xtdb.logging/set-log-level!")
private val createJobCalculator = requiringResolve("xtdb.compactor/->JobCalculator")
private val createTrieCatalog = requiringResolve("xtdb.trie-catalog/->TrieCatalog")

@ExtendWith(SeedLogger::class)
class SimulationTest {
    var currentSeed: Int = 0
    private lateinit var mockDriver: MockDriver
    private lateinit var jobCalculator: Compactor.JobCalculator
    private lateinit var compactor: Compactor.Impl
    private lateinit var trieCatalog: TrieCatalog
    private lateinit var db: MockDb

    @BeforeEach
    fun setUp() {
        setLogLevel.invoke("xtdb.compactor".symbol, logLevel)
        currentSeed = Random.nextInt()
        mockDriver = MockDriver(currentSeed)
        jobCalculator = createJobCalculator.invoke() as Compactor.JobCalculator
        compactor = Compactor.Impl(mockDriver, null, jobCalculator, false, 2)
        trieCatalog = createTrieCatalog.invoke(mutableMapOf<Any, Any>(), 100 * 1024 * 1024) as TrieCatalog
        db = MockDb("xtdb", trieCatalog)
    }

    @RepeatedTest(testIterations)
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @Disabled("Temporarily disabled due to timeouts")
    fun deterministicCompactorRun() {
        val docsTable = TableRef("xtdb", "public", "docs")
        val l0Trie = buildTrieDetails(docsTable.tableName, "l00-rc-b01")

        trieCatalog.addTries(docsTable, listOf(l0Trie), Instant.now())

        compactor.openForDatabase(db).use {
            it.compactAll()
        }

        Assertions.assertEquals(
            listOf("l00-rc-b01", "l01-rc-b01"),
            trieCatalog.listAllTrieKeys(docsTable),
            "Assertion failed for seed: $currentSeed"
        )
    }
}
