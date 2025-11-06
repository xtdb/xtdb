package xtdb.cache

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.cache.MemoryCache.Slice
import java.lang.Thread.sleep
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.JAVA_BYTE
import java.nio.file.Path
import java.util.concurrent.ExecutionException

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

                sleep(10)
                assertEquals(1, t1Evicted)

                cache.get(Path.of("t1/100"), Slice(0, 100)) { it to onEvict }.use { b1 ->
                    assertEquals(2, b1.getByte(0))
                }

                sleep(10)
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

                sleep(10)
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
}