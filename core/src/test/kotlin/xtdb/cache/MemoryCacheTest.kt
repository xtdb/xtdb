package xtdb.cache

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.cache.MemoryCache.Slice
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.JAVA_BYTE
import java.nio.file.Path
import java.util.concurrent.CompletableFuture.completedFuture
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
    fun `test memory cache`() {
        // just a starter-for-ten here, intent is that we go further down the property/deterministic testing route
        // significantly exercised E2E by the rest of the test-suite and benchmarks.

        // this used to be more of a test when the memory cache actually cached entries.
        // now, it's mostly a stats test.

        MemoryCache(allocator, 250, PathLoader()).use { cache ->

            var t1Evicted = false

            assertAll("get t1", {
                val onEvict = AutoCloseable { t1Evicted = true }

                cache.get(Path.of("t1/100"), Slice(0, 100)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(1, b1.getByte(0))

                    assertEquals(MemoryCache.Stats(100L, 150L), cache.stats0)
                }

                cache.get(Path.of("t1/100"), Slice(0, 100)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(2, b1.getByte(0))
                }

                Thread.sleep(50)
                assertEquals(MemoryCache.Stats(0, 250), cache.stats0)
                assertTrue(t1Evicted)
            })

            var t2Evicted = false

            assertAll("t2", {
                val onEvict = AutoCloseable { t2Evicted = true }

                val path = Path.of("t2/50")
                cache.get(path, Slice(0, 50)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(3, b1.getByte(0))

                    assertEquals(MemoryCache.Stats(50, 200), cache.stats0)
                }

                Thread.sleep(100)
                assertEquals(MemoryCache.Stats(0L, 250L), cache.stats0)
            })

            assertTrue(t1Evicted)
            assertTrue(t2Evicted)

            assertAll("t3 evicts t2/t1", {
                cache.get(Path.of("t3/170"), Slice(0, 170)) { completedFuture(it to null) }.use { b1 ->
                    assertEquals(4, b1.getByte(0))
                    assertTrue(t1Evicted)

                    // definitely needs to evict t1, may or may not evict t2
                    val stats = cache.stats0
                    assertEquals(170, stats.usedBytes)
                    assertEquals(80, stats.freeBytes)
                }

                Thread.sleep(100)
                val stats = cache.stats0
                assertEquals(0, stats.usedBytes)
                assertEquals(250, stats.freeBytes)
            })
        }
    }

    private inline fun <R> unwrapCause(f: () -> R) =
        try {
            f()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }

/*
    @Test
    fun `getting same path multiple times doesn't increase usedBytes`() {
        MemoryCache(allocator, 200, PathLoader()).use { cache ->
            val path = Path.of("test/100")
            val slice = Slice(0, 100)

            cache.get(path, slice) { completedFuture(it to null) }.use {
                assertEquals(100, cache.stats0.usedBytes)

                cache.get(path, slice) { completedFuture(it to null) }.use {
                    //assertEquals(100, cache.stats0.usedBytes)
                    // Fails -> comes back as 200 usedBytes, because we don't actually cache anything

                    cache.get(path, slice) { completedFuture(it to null) }.use {
                        assertEquals(100, cache.stats0.usedBytes)
                        // Fails -> throws an OOM error!
                    }
                }
            }

            Thread.sleep(100)
            val stats = cache.stats0
            assertEquals(0, stats.usedBytes)
            assertEquals(200, stats.freeBytes)
        }
    }
*/

    @Test
    fun `ooms the mem-cache`() {
        MemoryCache(allocator, 100, PathLoader()).use { cache ->
            assertThrows(OutOfMemoryException::class.java) {
                unwrapCause {
                    cache.get(Path.of("t1/200"), Slice(0, 200)) { k -> completedFuture(k to null) }.use { }
                }
            }

            assertAll(
                "only takes a slice of a bigger file",
                {
                    cache.get(Path.of("t1/200"), Slice(0, 50)) { k -> completedFuture(k to null) }.use { b1 ->
                        assertEquals(2, b1.getByte(0))
                    }
                }
            )

            assertAll(
                "but too many slices OOMs too",
                {
                    cache.get(Path.of("t1/200"), Slice(0, 75)) { k -> completedFuture(k to null) }.use { b1 ->
                        assertEquals(3, b1.getByte(0))

                        assertThrows(OutOfMemoryException::class.java) {
                            unwrapCause {
                                cache.get(Path.of("t1/200"), Slice(75, 75)) { k -> completedFuture(k to null) }.use {}
                            }
                        }
                    }
                }
            )
        }
    }
}