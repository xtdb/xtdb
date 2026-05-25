package xtdb.api.log

import kotlinx.coroutines.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import xtdb.util.MsgIdUtil.offsetToMsgId
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

/**
 * End-to-end tests against the real WorkerDO Durable Streams implementation
 * via the Node HTTP shim at `scratchpad/slice-1/worker/ds_node_shim.mjs`.
 *
 * Skipped automatically when `node` is not on PATH or the shim file is not
 * present — keeps CI green on developer machines that don't have the spike
 * checkout sitting next to this one.
 *
 * Override the shim location with the `XTDB_DS_NODE_SHIM` env var.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DurableStreamsLogIntegrationTest {

    private object StringCodec : MessageCodec<String> {
        override fun encode(message: String): ByteArray = message.toByteArray(Charsets.UTF_8)
        override fun decode(bytes: ByteArray): String = String(bytes, Charsets.UTF_8)
    }

    // One shim process for the whole class — each test uses a unique stream
    // path so the WorkerDO instances don't share state.
    private var shim: ShimProcess? = null
    private lateinit var log: DurableStreamsLog
    private lateinit var streamPath: String

    @BeforeAll
    fun startShim() {
        shim = ShimProcess.startIfAvailable()
    }

    @AfterAll
    fun stopShim() {
        shim?.stop()
    }

    @BeforeEach
    fun setUp() {
        assumeTrue(shim != null, "Node DS shim not available — set XTDB_DS_NODE_SHIM or install node")
        log = DurableStreamsLog(shim!!.baseUrl)
        streamPath = "/streams/xtdb/test-${System.nanoTime()}"
    }

    @AfterEach
    fun tearDown() {
        runCatching { log.close() }
    }

    private fun openLog(epoch: Int = 0): DurableStreamsLog.DsLog<String> {
        val url = "${shim!!.baseUrl}$streamPath"
        log.ensureStream(url)
        return log.DsLog(StringCodec, url, epoch)
    }

    @Test
    fun `appendMessage returns metadata with monotonic offsets`() = runBlocking {
        openLog().use { dsLog ->
            val m0 = dsLog.appendMessage("hello")
            val m1 = dsLog.appendMessage("world")
            val m2 = dsLog.appendMessage("xtdb")

            assertEquals(0L, m0.logOffset)
            assertEquals(1L, m1.logOffset)
            assertEquals(2L, m2.logOffset)
            assertEquals(2L, dsLog.latestSubmittedOffset)
        }
    }

    @Test
    fun `readLastMessage returns the most recent message`() = runBlocking {
        openLog().use { dsLog ->
            assertNull(dsLog.readLastMessage())
            dsLog.appendMessage("first")
            dsLog.appendMessage("second")
            assertEquals("second", dsLog.readLastMessage())
        }
    }

    @Test
    fun `readRecords returns messages in the half-open offset range`() = runBlocking {
        openLog().use { dsLog ->
            listOf("a", "b", "c", "d").forEach { dsLog.appendMessage(it) }

            val all = dsLog.readRecords(offsetToMsgId(0, 0), offsetToMsgId(0, 4)).toList()
            assertEquals(listOf("a", "b", "c", "d"), all.map { it.message })
            assertEquals(listOf(0L, 1L, 2L, 3L), all.map { it.logOffset })

            val slice = dsLog.readRecords(offsetToMsgId(0, 1), offsetToMsgId(0, 3)).toList()
            assertEquals(listOf("b", "c"), slice.map { it.message })
        }
    }

    @Test
    fun `readRecords returns empty when ranges are empty or cross-epoch`() = runBlocking {
        openLog(epoch = 2).use { dsLog ->
            dsLog.appendMessage("x")
            assertEquals(emptyList<Log.Record<String>>(),
                dsLog.readRecords(offsetToMsgId(0, 0), offsetToMsgId(0, 1)).toList())
            assertEquals(emptyList<Log.Record<String>>(),
                dsLog.readRecords(offsetToMsgId(2, 5), offsetToMsgId(2, 5)).toList())
        }
    }

    @Test
    fun `tailAll delivers records as they are appended`() = runBlocking {
        openLog().use { dsLog ->
            dsLog.appendMessage("warm-up")

            val received = CopyOnWriteArrayList<String>()
            val tail = launch(Dispatchers.IO) {
                dsLog.tailAll(offsetToMsgId(0, -1)) { records ->
                    records.forEach { received += it.message }
                }
            }
            try {
                withTimeout(10.seconds) {
                    while (received.size < 1) delay(20)
                }
                assertEquals(listOf("warm-up"), received.toList())

                dsLog.appendMessage("live-1")
                dsLog.appendMessage("live-2")
                withTimeout(10.seconds) {
                    while (received.size < 3) delay(20)
                }
                assertEquals(listOf("warm-up", "live-1", "live-2"), received.toList())
            } finally {
                tail.cancel()
                tail.join()
            }
        }
    }

    @Test
    fun `openAtomicProducer commits a buffered transaction`() = runBlocking {
        openLog().use { dsLog ->
            dsLog.openAtomicProducer("test-tx").use { producer ->
                val tx = producer.openTx()
                val d0 = tx.appendMessage("tx-a")
                val d1 = tx.appendMessage("tx-b")
                assertFalse(d0.isCompleted)
                assertFalse(d1.isCompleted)
                tx.commit()
                assertEquals(0L, d0.await().logOffset)
                assertEquals(1L, d1.await().logOffset)
            }
            assertEquals(listOf("tx-a", "tx-b"),
                dsLog.readRecords(offsetToMsgId(0, 0), offsetToMsgId(0, 2)).map { it.message }.toList())
        }
    }

    @Test
    fun `openAtomicProducer abort discards buffered messages`() = runBlocking {
        openLog().use { dsLog ->
            dsLog.openAtomicProducer("test-tx").use { producer ->
                val tx = producer.openTx()
                tx.appendMessage("dropped-1")
                tx.appendMessage("dropped-2")
                tx.abort()
            }
            assertNull(dsLog.readLastMessage())
            assertEquals(-1L, dsLog.latestSubmittedOffset)
        }
    }

    @Test
    fun `openAtomicProducer supports multiple sequential transactions`() = runBlocking {
        openLog().use { dsLog ->
            dsLog.openAtomicProducer("multi-tx").use { producer ->
                producer.openTx().also { tx ->
                    tx.appendMessage("one")
                    tx.appendMessage("two")
                    tx.commit()
                }
                producer.openTx().also { tx ->
                    tx.appendMessage("three")
                    tx.commit()
                }
            }
            assertEquals(2L, dsLog.latestSubmittedOffset)
            assertEquals(listOf("one", "two", "three"),
                dsLog.readRecords(offsetToMsgId(0, 0), offsetToMsgId(0, 3)).map { it.message }.toList())
        }
    }

    @Test
    fun `latestSubmittedOffset reflects existing stream state on open`() = runBlocking {
        openLog().use { dsLog ->
            dsLog.appendMessage("seed-0")
            dsLog.appendMessage("seed-1")
        }
        openLog().use { dsLog ->
            assertEquals(1L, dsLog.latestSubmittedOffset)
        }
    }

    @Test
    fun `openGroupSubscription tails the stream as partition 0`() = runBlocking {
        openLog().use { dsLog ->
            dsLog.appendMessage("g0")
            dsLog.appendMessage("g1")
            val received = CopyOnWriteArrayList<String>()
            val sub = launch(Dispatchers.IO) {
                dsLog.openGroupSubscription(object : Log.SubscriptionListener<String> {
                    override suspend fun onPartitionsAssigned(partitions: Collection<Int>): Log.TailSpec<String> {
                        assertEquals(listOf(0), partitions.toList())
                        return Log.TailSpec(offsetToMsgId(0, -1)) { records ->
                            records.forEach { received += it.message }
                        }
                    }
                    override suspend fun onPartitionsRevoked(partitions: Collection<Int>) = Unit
                })
            }
            try {
                withTimeout(10.seconds) { while (received.size < 2) delay(20) }
                assertEquals(listOf("g0", "g1"), received.toList())
            } finally {
                sub.cancel()
                sub.join()
            }
        }
    }
}

/**
 * Launches the scratchpad DS Node shim on a free port. The shim re-uses the
 * production WorkerDO implementation via in-process mock state, so tests run
 * against the same logic CF deploys — minus the DO storage layer.
 */
private class ShimProcess private constructor(
    val process: Process,
    val port: Int,
) {
    val baseUrl: String get() = "http://127.0.0.1:$port"

    fun stop() {
        if (!process.isAlive) return
        process.destroy()
        if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly()
    }

    companion object {
        private val SHIM_CANDIDATES = listOf(
            System.getenv("XTDB_DS_NODE_SHIM"),
            "/home/sprite/repos/scratchpad/slice-1/worker/ds_node_shim.mjs",
            "/home/hogan/scratchpad/slice-1/worker/ds_node_shim.mjs",
            System.getProperty("user.home") + "/repos/scratchpad/slice-1/worker/ds_node_shim.mjs",
        )

        fun startIfAvailable(): ShimProcess? {
            val nodeBin = locateNode() ?: return null
            val shimFile = SHIM_CANDIDATES
                .filterNotNull()
                .map(::File)
                .firstOrNull { it.isFile } ?: return null

            val port = freePort()
            val pb = ProcessBuilder(nodeBin, shimFile.absolutePath)
                .directory(shimFile.parentFile)
                .redirectErrorStream(true)
            pb.environment()["PORT"] = port.toString()
            pb.environment()["HOST"] = "127.0.0.1"
            val process = pb.start()

            return try {
                if (waitForReady(process, port)) ShimProcess(process, port)
                else {
                    process.destroyForcibly(); null
                }
            } catch (e: Throwable) {
                process.destroyForcibly(); throw e
            }
        }

        private fun locateNode(): String? {
            // Honour explicit override first.
            System.getenv("XTDB_NODE_BIN")?.takeIf { File(it).canExecute() }?.let { return it }
            // Fall back to walking PATH.
            val pathEnv = System.getenv("PATH") ?: return null
            return pathEnv.split(File.pathSeparator)
                .asSequence()
                .map { File(it, "node") }
                .firstOrNull { it.canExecute() }
                ?.absolutePath
        }

        private fun freePort(): Int = ServerSocket(0).use { it.localPort }

        private fun waitForReady(process: Process, port: Int): Boolean {
            // Drain output on a background thread so the shim doesn't block
            // on a full pipe — and so we can spot the "listening at" line.
            val reader = BufferedReader(InputStreamReader(process.inputStream))
            val readyMarker = "listening at"
            val deadline = System.nanoTime() + 10_000_000_000L
            val ready = java.util.concurrent.CountDownLatch(1)

            Thread {
                try {
                    reader.forEachLine { line ->
                        if (line.contains(readyMarker)) ready.countDown()
                    }
                } catch (_: Throwable) {
                    // Pipe closed when process dies; nothing to do.
                }
            }.apply { isDaemon = true }.start()

            val timeoutMs = ((deadline - System.nanoTime()) / 1_000_000L).coerceAtLeast(1)
            if (!ready.await(timeoutMs, TimeUnit.MILLISECONDS)) return false
            return process.isAlive
        }
    }
}
