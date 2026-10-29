package xtdb.cache

import io.kotest.assertions.throwables.shouldThrow
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.memory.rounding.RoundingPolicy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import xtdb.cache.MemoryCache.Slice
import xtdb.util.ReclaimingAllocationListener
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.writeBytes

class MemoryCacheTest {

    @TempDir
    lateinit var dir: Path

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = exactRootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    private fun content(len: Int, seed: Int = 0) = ByteArray(len) { ((it + seed * 17) % 251).toByte() }

    private fun file(name: String, len: Int, seed: Int = 0): Path =
        dir.resolve(name).also { it.writeBytes(content(len, seed)) }

    private class CountingFetch : MemoryCache.Fetch {
        val counts = mutableMapOf<Path, Int>()

        override suspend fun invoke(k: Path): Pair<Path, AutoCloseable?> {
            counts.merge(k, 1, Int::plus)
            return k to null
        }

        operator fun get(k: Path) = counts[k] ?: 0
    }

    private fun ArrowBuf.bytes() = ByteArray(capacity().toInt()).also { getBytes(0, it) }

    private fun exactRootAllocator(limit: Long = Long.MAX_VALUE) =
        RootAllocator(ReclaimingAllocationListener(), limit, RoundingPolicy { it })

    private fun cache(maxSizeBytes: Long, directIO: Boolean = true) =
        MemoryCache(allocator, maxSizeBytes, directIO = directIO)

    @Test
    fun `a page stays resident once released, and is read again without a fetch`() = runTest {
        val f = file("f", 100)
        val fetch = CountingFetch()

        cache(1000).use { cache ->
            cache.get(f, Slice(0, 100), fetch).use { assertArrayEquals(content(100), it.bytes()) }
            assertEquals(MemoryCache.Stats(100, 900), cache.stats0)

            cache.get(f, Slice(0, 100), fetch).use { assertArrayEquals(content(100), it.bytes()) }
            assertEquals(1, fetch[f])
            assertEquals(MemoryCache.Stats(100, 900), cache.stats0)
        }
    }

    @ParameterizedTest(name = "directIO = {0}")
    @ValueSource(booleans = [true, false])
    fun `reads the slice's bytes wherever it falls in the file`(directIO: Boolean) = runTest {
        val len = 20_000
        val f = file("f", len)
        val expected = content(len)

        cache(1L shl 20, directIO).use { cache ->
            for (slice in listOf(Slice(0, 1), Slice(4095, 2), Slice(4097, 8192), Slice(12_345, 7_000), Slice(19_999, 1), Slice(0, len.toLong()))) {
                cache.get(f, slice) { it to null }.use { buf ->
                    assertArrayEquals(
                        expected.copyOfRange(slice.offset.toInt(), (slice.offset + slice.length).toInt()), buf.bytes(),
                        "$slice"
                    )
                }
            }

            cache.get(f, null) { it to null }.use { assertArrayEquals(expected, it.bytes(), "whole file") }
        }
    }

    @Test
    fun `reads a file larger than one bounce buffer`() = runTest {
        val len = (3 shl 20) + 123
        val f = file("f", len)

        cache(8L shl 20).use { cache ->
            cache.get(f, Slice(7, len - 7L)) { it to null }.use { buf ->
                assertArrayEquals(content(len).copyOfRange(7, len), buf.bytes())
            }
        }
    }

    @Test
    fun `evicts released pages to make room`() = runTest {
        val fs = (0 until 3).map { file("f$it", 100, it) }
        val fetch = CountingFetch()

        cache(250).use { cache ->
            for (f in fs) cache.get(f, null, fetch).close()
            assertEquals(200, cache.stats0.usedBytes)

            cache.get(fs[0], null, fetch).use { assertArrayEquals(content(100, 0), it.bytes()) }
            assertEquals(2, fetch[fs[0]], "the oldest page was evicted, so it's read again")
        }
    }

    @Test
    fun `throws OOM while every page is held, and recovers once one is released`() = runTest {
        val fs = (0 until 3).map { file("f$it", 100, it) }

        cache(250).use { cache ->
            val b0 = cache.get(fs[0], null) { it to null }
            cache.get(fs[1], null) { it to null }.use {
                shouldThrow<OutOfMemoryException> { cache.get(fs[2], null) { it to null } }
            }

            cache.get(fs[2], null) { it to null }.use { assertArrayEquals(content(100, 2), it.bytes()) }
            assertArrayEquals(content(100, 0), b0.bytes())
            b0.close()
        }
    }

    @Test
    fun `evicts from the small queue when every page in the main queue is held`() = runTest {
        val hot = (0 until 9).map { file("hot$it", 100, it) }
        val c0 = file("c0", 100, 10)
        val c1 = file("c1", 50, 11)
        val c2 = file("c2", 100, 12)

        cache(1000).use { cache ->
            for (f in hot) repeat(2) { cache.get(f, null) { it to null }.close() }
            cache.get(c0, null) { it to null }.close()
            cache.get(c1, null) { it to null }.close()

            val held = hot.map { f -> cache.get(f, null) { it to null } }
            try {
                cache.get(c2, null) { it to null }.use { assertArrayEquals(content(100, 12), it.bytes()) }
            } finally {
                held.forEach { it.close() }
            }
        }
    }

    @Test
    fun `a page a reader has taken into its own allocator stays resident while the reader holds it`() = runTest {
        val fs = (0 until 4).map { file("f$it", 100, it) }
        val fetch = CountingFetch()

        cache(250).use { cache ->
            allocator.newChildAllocator("reader", 0, Long.MAX_VALUE).use { readerAl ->
                val taken = cache.get(fs[0], null, fetch).use { it.referenceManager.transferOwnership(it, readerAl).transferredBuffer }

                for (f in fs.drop(1)) cache.get(f, null, fetch).close()

                cache.get(fs[0], null, fetch).use { assertArrayEquals(content(100, 0), it.bytes()) }
                assertEquals(1, fetch[fs[0]])

                assertArrayEquals(content(100, 0), taken.bytes())
                taken.close()
            }
        }
    }

    @Test
    fun `gives back released pages when another allocation under the same root fails`() = runTest {
        val fs = (0 until 4).map { file("f$it", 100, it) }

        exactRootAllocator(500).use { root ->
            MemoryCache(root, 500).use { cache ->
                for (f in fs) cache.get(f, null) { it to null }.close()

                root.newChildAllocator("query", 0, Long.MAX_VALUE).use { queryAl ->
                    queryAl.buffer(300).close()
                }
                assertEquals(100, cache.stats0.usedBytes)
            }
        }
    }

    @Test
    fun `doesn't give back pages a reader holds`() = runTest {
        val fs = (0 until 4).map { file("f$it", 100, it) }

        exactRootAllocator(500).use { root ->
            MemoryCache(root, 500).use { cache ->
                val held = fs.map { f -> cache.get(f, null) { it to null } }
                try {
                    root.newChildAllocator("query", 0, Long.MAX_VALUE).use { queryAl ->
                        shouldThrow<OutOfMemoryException> { queryAl.buffer(300) }
                    }
                } finally {
                    held.forEach { it.close() }
                }
            }
        }
    }

    @Test
    fun `everyone waiting on a load that fails sees its failure`() = runTest {
        val f = file("f", 100)
        val fetches = AtomicInteger()

        cache(1000).use { cache ->
            val results = (0 until 3).map {
                async {
                    runCatching {
                        cache.get(f, null) {
                            fetches.incrementAndGet()
                            yield()
                            throw IOException("boom")
                        }
                    }
                }
            }.awaitAll()

            results.forEach { assertTrue(it.exceptionOrNull() is IOException, "$it") }
            assertEquals(1, fetches.get())
        }
    }

    @Test
    fun `a prefetched page is read without a fetch of its own`() = runTest {
        val f = file("f", 100)
        val fetch = CountingFetch()

        cache(1000).use { cache ->
            cache.prefetch(f, Slice(0, 100), fetch)
            cache.prefetch(f, Slice(0, 100), fetch)
            assertEquals(1, fetch[f])

            cache.get(f, Slice(0, 100), fetch).use { assertArrayEquals(content(100), it.bytes()) }
            assertEquals(1, fetch[f])
        }
    }

    @Test
    fun `a read while a prefetch is in flight waits for it rather than reading again`() = runTest {
        val f = file("f", 100)
        val fetches = AtomicInteger()
        val gate = CompletableDeferred<Unit>()

        cache(1000).use { cache ->
            val prefetch = async { cache.prefetch(f, null) { fetches.incrementAndGet(); gate.await(); it to null } }
            yield()

            val read = async { cache.get(f, null) { fetches.incrementAndGet(); it to null }.use { it.bytes() } }
            yield()
            gate.complete(Unit)

            prefetch.await()
            assertArrayEquals(content(100), read.await())
            assertEquals(1, fetches.get())
        }
    }

    @Test
    fun `a prefetched page read once is evicted like any page read once`() = runTest {
        val f = file("f", 100)
        val others = (0 until 3).map { file("o$it", 100, it + 1) }
        val fetch = CountingFetch()

        cache(300).use { cache ->
            cache.prefetch(f, null, fetch)
            cache.get(f, null, fetch).close()

            for (o in others) cache.get(o, null, fetch).close()

            cache.get(f, null, fetch).close()
            assertEquals(2, fetch[f], "read once, so it was the first evicted")
        }
    }

    @Test
    fun `prefetches read a few pages at a time, and a read doesn't wait for them`() = runTest {
        val pages = (0 until 3 * MemoryCache.PREFETCH_PARALLELISM).map { file("p$it", 100, it) }
        val g = file("g", 100)
        val fetches = AtomicInteger()
        val gate = CompletableDeferred<Unit>()

        cache(10_000).use { cache ->
            val prefetches = pages.map { p -> async { cache.prefetch(p, null) { fetches.incrementAndGet(); gate.await(); it to null } } }
            yield()
            assertEquals(MemoryCache.PREFETCH_PARALLELISM, fetches.get())

            cache.get(g, null) { it to null }.use { assertArrayEquals(content(100), it.bytes()) }

            gate.complete(Unit)
            prefetches.awaitAll()
            assertEquals(pages.size, fetches.get())
        }
    }

    @Test
    fun `a read waiting on a prefetch that fails reads the page itself`() = runTest {
        val f = file("f", 100)
        val fetches = AtomicInteger()
        val gate = CompletableDeferred<Unit>()

        cache(1000).use { cache ->
            val prefetch = async {
                runCatching { cache.prefetch(f, null) { fetches.incrementAndGet(); gate.await(); throw IOException("boom") } }
            }
            yield()

            val read = async { cache.get(f, null) { fetches.incrementAndGet(); it to null }.use { it.bytes() } }
            yield()
            gate.complete(Unit)

            assertTrue(prefetch.await().isFailure)
            assertArrayEquals(content(100), read.await())
            assertEquals(2, fetches.get())
        }
    }

    @Test
    fun `a prefetched page evicted before it's read is read again as if for the first time`() = runTest {
        val f = file("f", 100)
        val others = (0 until 6).map { file("o$it", 100, it + 1) }
        val fetch = CountingFetch()

        cache(300).use { cache ->
            cache.prefetch(f, null, fetch)
            for (o in others.take(3)) cache.get(o, null, fetch).close()

            cache.get(f, null, fetch).close()
            assertEquals(2, fetch[f])

            for (o in others.drop(3)) cache.get(o, null, fetch).close()

            cache.get(f, null, fetch).close()
            assertEquals(3, fetch[f], "read once since it was evicted, so evicted again")
        }
    }

    @Test
    fun `throws OOM for a page larger than the cache`() = runTest {
        val big = file("big", 200)
        val small = file("small", 50)

        cache(100).use { cache ->
            shouldThrow<OutOfMemoryException> { cache.get(big, null) { it to null } }

            cache.get(big, Slice(0, 50)) { it to null }.use { assertArrayEquals(content(50), it.bytes()) }
            cache.get(small, null) { it to null }.use { assertArrayEquals(content(50), it.bytes()) }
        }
    }

    @Test
    fun `closes the fetch's on-read action once the page is read`() = runTest {
        val f = file("f", 100)
        var closed = 0

        cache(1000).use { cache ->
            cache.get(f, null) { it to AutoCloseable { closed++ } }.use {
                assertEquals(1, closed)
            }
        }
    }

    @Test
    fun `a failed fetch leaves nothing behind`() = runTest {
        val f = file("f", 100)

        cache(1000).use { cache ->
            shouldThrow<IOException> { cache.get(f, null) { throw IOException("boom") } }
            assertEquals(MemoryCache.Stats(0, 1000), cache.stats0)

            cache.get(f, null) { it to null }.use { assertArrayEquals(content(100), it.bytes()) }
        }
    }

    @Test
    fun `a page read again survives a scan of pages read once`() = runTest {
        val hot = file("hot", 100)
        val cold = (0 until 20).map { file("cold$it", 100, it + 1) }
        val fetch = CountingFetch()

        cache(1000).use { cache ->
            repeat(2) { cache.get(hot, null, fetch).close() }
            for (f in cold) cache.get(f, null, fetch).close()

            cache.get(hot, null, fetch).use { assertArrayEquals(content(100), it.bytes()) }
            assertEquals(1, fetch[hot])
        }
    }
}
