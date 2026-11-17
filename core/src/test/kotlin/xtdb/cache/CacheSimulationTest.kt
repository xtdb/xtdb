package xtdb.cache

import io.kotest.assertions.throwables.shouldThrow
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.OutOfMemoryException
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.memory.rounding.RoundingPolicy
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.cache.MemoryCache.Slice
import xtdb.util.ReclaimingAllocationListener
import xtdb.symbol
import xtdb.util.requiringResolve
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.io.path.writeBytes

private val setLogLevel = requiringResolve("xtdb.logging/set-log-level!")

private const val logLevel = "WARN"

@Tag("property")
class CacheSimulationTest : SimulationTestBase() {

    @TempDir
    lateinit var dir: Path

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUpSimulation() {
        setLogLevel.invoke("xtdb.cache".symbol, logLevel)
        allocator = RootAllocator(ReclaimingAllocationListener(), Long.MAX_VALUE, RoundingPolicy { it })
    }

    @AfterEach
    fun tearDownSimulation() {
        allocator.close()
    }

    private fun content(len: Int, seed: Int) = ByteArray(len) { ((it + seed * 17) % 251).toByte() }

    private fun file(name: String, len: Int, seed: Int): Path =
        dir.resolve(name).also { it.writeBytes(content(len, seed)) }

    private fun ArrowBuf.bytes() = ByteArray(capacity().toInt()).also { getBytes(0, it) }

    private inner class CountingFetch : MemoryCache.Fetch {
        val counts = mutableMapOf<Path, Int>()

        override suspend fun invoke(k: Path): Pair<Path, AutoCloseable?> {
            counts.merge(k, 1, Int::plus)
            yield()
            return k to null
        }

        operator fun get(k: Path) = counts[k] ?: 0
    }

    private fun cache(maxSizeBytes: Long) = MemoryCache(allocator, maxSizeBytes, dispatcher, directIO = rand.nextBoolean())

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `concurrent fetches of one page read it once`() = runTest {
        val len = rand.nextInt(1, 250)
        val f = file("f", len, 0)
        val fetch = CountingFetch()

        cache(250).use { cache ->
            val results = async(dispatcher) {
                (1..rand.nextInt(2, 10)).map {
                    async(dispatcher) {
                        cache.get(f, Slice(0, len.toLong()), fetch).use { buf -> yield(); buf.bytes() }
                    }
                }.awaitAll()
            }.await()

            results.forEach { assertArrayEquals(content(len, 0), it) }
            assertEquals(1, fetch[f])
            assertEquals(len.toLong(), cache.stats0.usedBytes)
        }
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `concurrent fetches of different slices of one file`() = runTest {
        val n = rand.nextInt(2, 10)
        val f = file("f", n * 50, 0)
        val expected = content(n * 50, 0)

        cache(1000).use { cache ->
            val results = async(dispatcher) {
                (0 until n).map { i ->
                    async(dispatcher) {
                        cache.get(f, Slice(i * 50L, 50)) { it to null }.use { buf -> yield(); buf.bytes() }
                    }
                }.awaitAll()
            }.await()

            results.forEachIndexed { i, bytes -> assertArrayEquals(expected.copyOfRange(i * 50, i * 50 + 50), bytes) }
        }
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `cancelled fetches leave nothing held`() = runTest {
        val f = file("f", 100, 0)

        cache(250).use { cache ->
            async(dispatcher) {
                val jobs = (0..1).map {
                    launch { cache.get(f, null) { it to null }.use { yield() } }
                }
                yield()
                jobs.forEach { it.cancel() }
                jobs.joinAll()
            }.await()

            // a page still held would fail this OOM
            cache.get(file("g", 250, 1), null) { it to null }.use { assertArrayEquals(content(250, 1), it.bytes()) }
        }
    }

    @RepeatableSimulationTest
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `pages larger than the cache fail without disturbing the rest`() = runTest {
        cache(100).use { cache ->
            val results = async(dispatcher) {
                (0 until rand.nextInt(4, 10)).map { i ->
                    val len = if (i % 2 == 0) 10 else 150
                    val f = file("f$i", len, i)
                    async(dispatcher) {
                        runCatching { cache.get(f, null) { it to null }.use { buf -> yield(); buf.bytes() } }
                    }
                }.awaitAll()
            }.await()

            results.forEachIndexed { i, result ->
                if (i % 2 == 0) assertArrayEquals(content(10, i), result.getOrThrow())
                else assertTrue(result.exceptionOrNull() is OutOfMemoryException, "fetch $i: $result")
            }

            shouldThrow<OutOfMemoryException> { cache.get(file("big", 101, 0), null) { it to null } }
        }
    }

    @RepeatableSimulationTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `random workload reads correct bytes within the budget`() = runTest {
        val maxSizeBytes = rand.nextLong(500, 5000)
        val files = (0 until rand.nextInt(5, 30)).map { i ->
            val len = rand.nextInt(1, 600)
            Triple(file("f$i", len, i), len, i)
        }

        var heldBytes = 0L

        cache(maxSizeBytes).use { cache ->
            async(dispatcher) {
                (0 until rand.nextInt(1, 6)).map {
                    async(dispatcher) {
                        repeat(rand.nextInt(10, 50)) {
                            val (f, len, seed) = files[rand.nextInt(files.size)]
                            val offset = rand.nextInt(0, len)
                            val sliceLen = rand.nextInt(1, len - offset + 1)

                            try {
                                cache.get(f, Slice(offset.toLong(), sliceLen.toLong())) { yield(); it to null }.use { buf ->
                                    heldBytes += sliceLen
                                    assertTrue(cache.stats0.usedBytes <= maxSizeBytes)
                                    repeat(rand.nextInt(0, 3)) { yield() }
                                    assertArrayEquals(content(len, seed).copyOfRange(offset, offset + sliceLen), buf.bytes())
                                    heldBytes -= sliceLen
                                }
                            } catch (e: OutOfMemoryException) {
                                assertTrue(heldBytes + sliceLen > maxSizeBytes, "OOM with $heldBytes held of $maxSizeBytes: $e")
                            }
                        }
                    }
                }.awaitAll()
            }.await()
        }
    }
}
