package xtdb.cache

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import xtdb.cache.MemoryCache.Slice
import java.io.IOException
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.JAVA_BYTE
import java.nio.file.Path
import java.util.concurrent.ExecutionException
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

        override fun load(path: Path, slice: Slice, arena: Arena): MemorySegment =
            arena.allocate(slice.length)
                .also { it.set(JAVA_BYTE, 0, (++idx).toByte()) }
    }

    @Test
    fun `test memory cache`() = runTest {
        // just a starter-for-ten here, intent is that we go further down the property/deterministic testing route
        // significantly exercised E2E by the rest of the test-suite and benchmarks.

        // this used to be more of a test when the memory cache actually cached entries.
        // now, it's mostly a stats test.

        MemoryCache(allocator, 250, PathLoader()).use { cache ->

            var t1Evicted = 0

            withClue("get t1") {
                val onEvict = AutoCloseable { t1Evicted++ }

                cache.get(Path.of("t1/100"), Slice(0, 100)) { it to onEvict }.use { b1 ->
                    assertEquals(1, b1.getByte(0))

                    assertEquals(MemoryCache.Stats(100L, 150L), cache.stats0)

                    cache.get(Path.of("t1/100"), Slice(0, 100)) { it to onEvict }.use { b1 ->
                        assertEquals(1, b1.getByte(0))

                        assertEquals(MemoryCache.Stats(100L, 150L), cache.stats0)
                    }
                }

                assertEquals(1, t1Evicted)

                cache.get(Path.of("t1/100"), Slice(0, 100)) { it to onEvict }.use { b1 ->
                    assertEquals(2, b1.getByte(0))
                }

                assertEquals(MemoryCache.Stats(0, 250), cache.stats0)
                assertEquals(2, t1Evicted)
            }

            var t2Evicted = false

            withClue("t2") {
                val onEvict = AutoCloseable {
                    t2Evicted = true
                }

                val path = Path.of("t2/50")
                cache.get(path, Slice(0, 50)) { it to onEvict }.use { b1 ->
                    assertEquals(3, b1.getByte(0))
                    assertEquals(MemoryCache.Stats(50, 200), cache.stats0)
                }

                assertTrue(t2Evicted)
                assertEquals(2, t1Evicted)
                assertEquals(MemoryCache.Stats(0L, 250L), cache.stats0)
            }
        }
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
                    assertEquals(3, b1.getByte(0))

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

}