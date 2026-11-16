package xtdb.cache

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.apache.arrow.memory.AllocationListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import xtdb.api.error.Fault
import xtdb.cache.MemoryCache.Slice
import java.io.IOException
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.JAVA_BYTE
import java.nio.file.Path
import java.util.concurrent.ExecutionException
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

class MemoryCacheTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    class PathLoader : MemoryCache.PathLoader {
        private var idx = 0

        // a file named for a number is that many bytes long
        override fun load(path: Path, arena: Arena): MemorySegment =
            arena.allocate(path.fileName.toString().toLongOrNull() ?: 1000)
                .also { it.set(JAVA_BYTE, 0, (++idx).toByte()) }
    }

    @Test
    fun `an idle file stays mapped and is served without re-fetching`() = runTest {
        MemoryCache(allocator, 250, PathLoader()).use { cache ->
            var fetches = 0
            var evictions = 0
            val fetch = MemoryCache.Fetch { fetches++; it to AutoCloseable { evictions++ } }
            val path = Path.of("t1/100")

            cache.get(path, Slice(0, 100), fetch).use { buf ->
                assertEquals(1, buf.getByte(0))
                assertEquals(MemoryCache.Stats(100, 150), cache.stats0)
            }

            assertEquals(MemoryCache.Stats(0, 250), cache.stats0)

            cache.get(path, Slice(0, 100), fetch).use { buf ->
                assertEquals(1, buf.getByte(0), "served from the same mapping")
            }

            assertEquals(1, fetches)
            assertEquals(0, evictions)
        }
    }

    @Test
    fun `a second slice of a mapped file doesn't re-fetch`() = runTest {
        MemoryCache(allocator, 250, PathLoader()).use { cache ->
            var fetches = 0
            val fetch = MemoryCache.Fetch { fetches++; it to null }
            val path = Path.of("t1/200")

            cache.get(path, Slice(0, 50), fetch).use { b1 ->
                cache.get(path, Slice(100, 50), fetch).use { b2 ->
                    assertEquals(1, b1.getByte(0))
                    assertEquals(0, b2.getByte(0), "the second slice starts 100 bytes into the same mapping")
                    assertEquals(MemoryCache.Stats(100, 150), cache.stats0)
                }
            }

            assertEquals(1, fetches)
        }
    }

    @Test
    fun `a slice past the end of its file fails`() = runTest {
        MemoryCache(allocator, 250, PathLoader()).use { cache ->
            shouldThrow<Fault> {
                cache.get(Path.of("t1/100"), Slice(50, 100)) { it to null }
            }

            shouldThrow<Fault> {
                cache.get(Path.of("t1/100"), Slice(Long.MAX_VALUE, 10)) { it to null }
            }

            shouldThrow<Fault> {
                cache.get(Path.of("t1/100"), Slice(50, Long.MAX_VALUE)) { it to null }
            }

            assertEquals(MemoryCache.Stats(0, 250), cache.stats0)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `evicts the least recently used idle file past the byte bound`() = runTest {
        MemoryCache(allocator, 250, PathLoader(), UnconfinedTestDispatcher(testScheduler)).use { cache ->
            val evicted = mutableListOf<String>()

            suspend fun get(p: String, inUse: suspend () -> Unit = {}) {
                cache.get(Path.of(p), Slice(0, 100)) { it to AutoCloseable { evicted.add(p) } }.use { inUse() }
                runCurrent()
            }

            get("a/100")
            get("b/100")
            assertEquals(emptyList<String>(), evicted)

            // with c in use, a and b (200 bytes idle) outgrow the 150 left
            get("c/100")
            assertEquals(listOf("a/100"), evicted)

            // with c and d in use, b outgrows the 50 left
            get("c/100") { get("d/100") }
            assertEquals(listOf("a/100", "b/100"), evicted)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `evicts the least recently used idle file past the file-count bound`() = runTest {
        MemoryCache(allocator, Long.MAX_VALUE, PathLoader(), UnconfinedTestDispatcher(testScheduler)).use { cache ->
            val evicted = mutableListOf<String>()

            repeat(MAX_IDLE_FILES + 2) { i ->
                val p = "f$i/1"
                cache.get(Path.of(p), Slice(0, 1)) { it to AutoCloseable { evicted.add(p) } }.use { }
                runCurrent()
            }

            assertEquals(listOf("f0/1"), evicted)
        }
    }

    @Test
    fun `close unmaps every idle file and releases its pin`() = runTest {
        val evicted = mutableListOf<String>()

        MemoryCache(allocator, 250, PathLoader()).use { cache ->
            for (p in listOf("a/50", "b/50")) {
                cache.get(Path.of(p), Slice(0, 50)) { it to AutoCloseable { evicted.add(p) } }.use { }
            }
            assertEquals(emptyList<String>(), evicted)
        }

        assertEquals(listOf("a/50", "b/50"), evicted)
    }

    private inline fun <R> unwrapCause(f: () -> R) =
        try {
            f()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }

    @Test
    fun `ooms the mem-cache`() = runTest {
        MemoryCache(allocator, 100, PathLoader()).use { cache ->
            shouldThrow<OutOfMemoryException> {
                unwrapCause {
                    cache.get(Path.of("t1/200"), Slice(0, 200)) { k -> k to null }.use { }
                }
            }

            withClue("only takes a slice of a bigger file") {
                cache.get(Path.of("t1/200"), Slice(0, 50)) { k -> k to null }.use { b1 ->
                    assertEquals(2, b1.getByte(0))
                }
            }

            withClue("but too many slices OOMs too") {
                cache.get(Path.of("t1/200"), Slice(0, 75)) { k -> k to null }.use { b1 ->
                    assertEquals(2, b1.getByte(0), "the second get's mapping, still idle")

                    shouldThrow<OutOfMemoryException> {
                        unwrapCause {
                            cache.get(Path.of("t1/200"), Slice(75, 75)) { k -> k to null }.use {}
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `a slice whose wrap fails after reserving its bytes leaves its file idle`() = runTest {
        val evicted = mutableListOf<String>()
        val failingListener = object : AllocationListener {
            override fun onAllocation(size: Long) = throw IllegalStateException("onAllocation failed")
        }

        RootAllocator(failingListener, Long.MAX_VALUE).use { al ->
            MemoryCache(al, 250, PathLoader()).use { cache ->
                shouldThrow<IllegalStateException> {
                    cache.get(Path.of("t1/100"), Slice(0, 50)) { it to AutoCloseable { evicted.add("t1/100") } }
                }

                assertEquals(MemoryCache.Stats(0, 250), cache.stats0)
            }
        }

        assertEquals(listOf("t1/100"), evicted, "close unmaps the file as idle")
    }

    @Test
    fun `getting same path multiple times doesn't increase usedBytes`() = runTest {
        MemoryCache(allocator, 200, PathLoader()).use { cache ->
            val path = Path.of("test/100")
            val slice = Slice(0, 100)

            cache.get(path, slice) { it to null }.use {
                assertEquals(100, cache.stats0.usedBytes)

                cache.get(path, slice) { it to null }.use {
                    // previously came back as 200 usedBytes, because we didn't actually cache anything
                    assertEquals(100, cache.stats0.usedBytes)

                    cache.get(path, slice) { it to null }.use {
                        assertEquals(100, cache.stats0.usedBytes)
                        // Fails -> throws an OOM error!
                    }
                }
            }

            val stats = cache.stats0
            assertEquals(0, stats.usedBytes)
            assertEquals(200, stats.freeBytes)
        }
    }

    private fun Throwable?.isOrCausedBy(expected: Throwable) =
        generateSequence(this) { it.cause }.any { it === expected }

    @OptIn(ExperimentalCoroutinesApi::class)
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `failed fetch releases concurrent waiters and permits retry`(cancelFirstWaiter: Boolean) = runTest {
        MemoryCache(allocator, 250, PathLoader(), UnconfinedTestDispatcher(testScheduler)).use { cache ->
            withTimeout(5.seconds) {
                val path = Path.of("failed-fetch")
                val slice = Slice(0, 100)
                val started = CompletableDeferred<Unit>()
                val release = CompletableDeferred<Unit>()
                val failure = IOException("object download failed")
                var attempts = 0
                val fetch = MemoryCache.Fetch {
                    if (++attempts == 1) {
                        started.complete(Unit)
                        release.await()
                        throw failure
                    }
                    it to null
                }

                val first = async { runCatching { cache.get(path, slice, fetch) } }
                started.await()
                val second = async { runCatching { cache.get(path, slice, fetch) } }
                runCurrent()
                assertEquals(1, attempts)

                if (cancelFirstWaiter) first.cancelAndJoin()
                release.complete(Unit)
                if (!cancelFirstWaiter) assertTrue(first.await().exceptionOrNull().isOrCausedBy(failure))
                assertTrue(second.await().exceptionOrNull().isOrCausedBy(failure))

                cache.get(path, slice, fetch).use { buf ->
                    assertEquals(100, buf.readableBytes())
                }
                assertEquals(2, attempts)
                assertEquals(MemoryCache.Stats(0, 250), cache.stats0)
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `cancelled fetch releases its waiter and permits retry`(cancelJob: Boolean) = runTest {
        MemoryCache(allocator, 250, PathLoader(), UnconfinedTestDispatcher(testScheduler)).use { cache ->
            withTimeout(5.seconds) {
                val path = Path.of("cancelled-fetch")
                val slice = Slice(0, 100)
                val failure = CancellationException("object download cancelled")

                assertTrue(shouldThrow<CancellationException> {
                    cache.get(path, slice) {
                        if (cancelJob) {
                            currentCoroutineContext().cancel(failure)
                            currentCoroutineContext().ensureActive()
                        }
                        throw failure
                    }
                }.isOrCausedBy(failure))
                cache.get(path, slice) { it to null }.use { buf ->
                    assertEquals(100, buf.readableBytes())
                }
                assertEquals(MemoryCache.Stats(0, 250), cache.stats0)
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `closing cache cancels a pending fetch`() = runTest {
        MemoryCache(allocator, 250, PathLoader(), UnconfinedTestDispatcher(testScheduler)).use { cache ->
            val started = CompletableDeferred<Unit>()
            val reader = async {
                cache.get(Path.of("pending-fetch"), Slice(0, 100)) {
                    started.complete(Unit)
                    awaitCancellation()
                }.use { }
            }
            started.await()
            reader.cancelAndJoin()
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @Timeout(10)
    fun `a fetch loop that dies fails its callers rather than hanging them`() {
        val oom = OutOfMemoryError("fetch loop OOM")

        val uncaught = assertThrows<OutOfMemoryError> {
            runTest {
                val testDispatcher = StandardTestDispatcher(testScheduler)

                val dispatcher = object : CoroutineDispatcher() {
                    override fun dispatch(context: CoroutineContext, block: Runnable) {
                        if (context[CoroutineName]?.name?.contains("oom") == true) throw oom
                        testDispatcher.dispatch(context, block)
                    }
                }

                MemoryCache(allocator, 250, PathLoader(), dispatcher).use { cache ->
                    withTimeout(5.seconds) {
                        val slice = Slice(0, 50)

                        cache.get(Path.of("open"), slice) { it to null }.use {
                            val started = CompletableDeferred<Unit>()
                            val inFlight = async {
                                runCatching {
                                    cache.get(Path.of("in-flight"), slice) {
                                        started.complete(Unit)
                                        awaitCancellation()
                                    }
                                }
                            }
                            started.await()

                            val dying = async { runCatching { cache.get(Path.of("oom"), slice) { it to null } } }
                            val queued = async { runCatching { cache.get(Path.of("queued"), slice) { it to null } } }

                            assertTrue(dying.await().exceptionOrNull().isOrCausedBy(oom))
                            assertTrue(queued.await().exceptionOrNull().isOrCausedBy(oom))
                            assertTrue(inFlight.await().exceptionOrNull().isOrCausedBy(oom))
                            assertTrue(runCatching { cache.get(Path.of("later"), slice) { it to null } }.exceptionOrNull().isOrCausedBy(oom))

                            cache.get(Path.of("open"), slice) { error("open slice re-fetched") }.use { buf ->
                                assertEquals(1, buf.getByte(0))
                            }
                        }
                    }
                }
            }
        }

        assertTrue(uncaught.isOrCausedBy(oom))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @Timeout(10)
    fun `a fetch that completes after the fetch loop dies closes its onEvict once`() {
        val oom = OutOfMemoryError("fetch loop OOM")

        val uncaught = assertThrows<OutOfMemoryError> {
            runTest {
                val testDispatcher = StandardTestDispatcher(testScheduler)

                val dispatcher = object : CoroutineDispatcher() {
                    override fun dispatch(context: CoroutineContext, block: Runnable) {
                        if (context[CoroutineName]?.name?.contains("oom") == true) throw oom
                        testDispatcher.dispatch(context, block)
                    }
                }

                MemoryCache(allocator, 250, PathLoader(), dispatcher).use { cache ->
                    withTimeout(5.seconds) {
                        val slice = Slice(0, 50)
                        val started = CompletableDeferred<Unit>()
                        val release = CompletableDeferred<Unit>()
                        var evictions = 0

                        val inFlight = async {
                            runCatching {
                                cache.get(Path.of("in-flight"), slice) {
                                    started.complete(Unit)
                                    withContext(NonCancellable) { release.await() }
                                    it to AutoCloseable { evictions++ }
                                }
                            }
                        }
                        started.await()

                        assertTrue(runCatching { cache.get(Path.of("oom"), slice) { it to null } }.exceptionOrNull().isOrCausedBy(oom))
                        assertTrue(inFlight.await().exceptionOrNull().isOrCausedBy(oom))

                        release.complete(Unit)
                        runCurrent()

                        assertEquals(1, evictions)
                    }
                }
            }
        }

        assertTrue(uncaught.isOrCausedBy(oom))
    }
}