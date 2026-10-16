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

        override fun load(path: Path, arena: Arena): MemorySegment =
            arena.allocate(path.fileName.toString().toLongOrNull() ?: 1000)
                .also { it.set(JAVA_BYTE, 0, (++idx).toByte()) }
    }

    @Test
    fun `a file stays mapped once its slices are released, until the cache closes`() = runTest {
        var t1Unmapped = 0
        var t2Unmapped = 0

        MemoryCache(allocator, 250, PathLoader()).use { cache ->
            withClue("t1") {
                val onEvict = AutoCloseable { t1Unmapped++ }

                cache.get(Path.of("t1/100"), Slice(0, 100)) { it to onEvict }.use { b1 ->
                    assertEquals(1, b1.getByte(0))
                    assertEquals(MemoryCache.Stats(100L, 150L), cache.stats0)

                    cache.get(Path.of("t1/100"), Slice(0, 100)) { it to onEvict }.use { b1 ->
                        assertEquals(1, b1.getByte(0))
                        assertEquals(MemoryCache.Stats(100L, 150L), cache.stats0)
                    }
                }

                assertEquals(MemoryCache.Stats(0, 250), cache.stats0)

                cache.get(Path.of("t1/100"), Slice(0, 100)) { it to onEvict }.use { b1 ->
                    assertEquals(1, b1.getByte(0), "the same mapping")
                }

                assertEquals(0, t1Unmapped)
            }

            withClue("t2") {
                val onEvict = AutoCloseable { t2Unmapped++ }

                cache.get(Path.of("t2/50"), Slice(0, 50)) { it to onEvict }.use { b2 ->
                    assertEquals(2, b2.getByte(0))
                    assertEquals(MemoryCache.Stats(50, 200), cache.stats0)
                }

                assertEquals(MemoryCache.Stats(0L, 250L), cache.stats0)
                assertEquals(0, t2Unmapped)
            }
        }

        assertEquals(1, t1Unmapped)
        assertEquals(1, t2Unmapped)
    }

    @Test
    fun `invalidating a prefix unmaps its idle files, and a later get fetches afresh`() = runTest {
        var fetches = 0
        var unmapped = 0
        val fetch = MemoryCache.Fetch { k -> fetches++; k to AutoCloseable { unmapped++ } }

        MemoryCache(allocator, 250, PathLoader()).use { cache ->
            cache.get(Path.of("db1/0/100"), Slice(0, 10), fetch).close()
            cache.get(Path.of("db2/0/100"), Slice(0, 10), fetch).close()

            cache.get(Path.of("db1/0/100"), Slice(0, 10), fetch).use {
                cache.invalidate(Path.of("db1"))
                assertEquals(0, unmapped, "a file with a slice in use stays mapped")
            }

            assertEquals(1, unmapped)

            cache.get(Path.of("db1/0/100"), Slice(0, 10), fetch).close()
            cache.get(Path.of("db2/0/100"), Slice(0, 10), fetch).close()
            assertEquals(3, fetches)
        }

        assertEquals(3, unmapped)
    }

    @Test
    fun `slices of one file share one mapping`() = runTest {
        var loads = 0
        val loader = object : MemoryCache.PathLoader {
            override fun load(path: Path, arena: Arena): MemorySegment {
                loads++
                return arena.allocate(100)
            }
        }

        MemoryCache(allocator, 200, loader).use { cache ->
            cache.get(Path.of("t1"), Slice(0, 50)) { it to null }.use { b1 ->
                cache.get(Path.of("t1"), Slice(50, 50)) { it to null }.use { b2 ->
                    assertEquals(50, b1.capacity())
                    assertEquals(50, b2.capacity())
                    assertEquals(MemoryCache.Stats(100, 100), cache.stats0)
                }
            }

            assertEquals(1, loads)
        }
    }

    @Test
    fun `a file larger than the cache can be read, and is unmapped once its slices are released`() = runTest {
        var unmapped = 0

        MemoryCache(allocator, 100, PathLoader()).use { cache ->
            cache.get(Path.of("t1/200"), Slice(150, 50)) { it to AutoCloseable { unmapped++ } }.use { b1 ->
                assertEquals(50, b1.capacity())
                assertEquals(0, unmapped)
            }

            assertEquals(1, unmapped)
            assertEquals(MemoryCache.Stats(0, 100), cache.stats0)
        }
    }

    private inline fun <R> unwrapCause(f: () -> R) =
        try {
            f()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }

    @Test
    fun `a failed fetch fails its waiter, and the slice can be fetched again`() = runTest {
        MemoryCache(allocator, 250, PathLoader()).use { cache ->
            val path = Path.of("t1")
            val slice = Slice(0, 100)

            val ex = shouldThrow<IllegalStateException> { cache.get(path, slice) { error("no such object") } }
            assertEquals("no such object", ex.message)

            cache.get(path, slice) { it to null }.use { buf -> assertEquals(100L, buf.readableBytes()) }

            assertEquals(MemoryCache.Stats(0, 250), cache.stats0)
        }
    }

    @Test
    fun `ooms the mem-cache`() = runTest {
        MemoryCache(allocator, 100, PathLoader()).use { cache ->
            shouldThrow<OutOfMemoryException> {
                unwrapCause {
                    cache.get(Path.of("t1/200"), Slice(0, 200)) { k -> k to null }.use { }
                }
            }

            withClue("takes a slice of a file bigger than the cache") {
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