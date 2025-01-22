package xtdb.cache

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.pathString

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

    inner class PathLoader : MemoryCache.PathLoader {
        private var idx = 0
        override fun load(path: Path): ByteBuf =
            Unpooled.directBuffer(path.last().pathString.toInt())
                .also { it.setByte(0, ++idx) }

        override fun load(pathSlice: PathSlice): ByteBuf =
            Unpooled.directBuffer(pathSlice.path.last().pathString.toInt())
                .also { it.setByte(0, ++idx) }
    }

    @Test
    fun `test memory cache`() {
        // just a starter-for-ten here, intent is that we go further down the property/deterministic testing route
        // significantly exercised E2E by the rest of the test-suite and benchmarks.

        MemoryCache(allocator, 250, PathLoader()).use { cache ->

            var t1Evicted = false

            assertAll("get t1", {
                val onEvict = AutoCloseable { t1Evicted = true }

                cache.get(PathSlice(Path.of("t1/100"), 0, 100)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(1, b1.getByte(0))

                    assertEquals(PinningCache.Stats(100L, 0L, 150L), cache.pinningCache.stats0)
                }

                cache.get(PathSlice(Path.of("t1/100"), 0, 100)) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(1, b1.getByte(0))
                }

                Thread.sleep(50)
                assertEquals(PinningCache.Stats(0L, 100L, 150L), cache.pinningCache.stats0)
                assertFalse(t1Evicted)
            })

            var t2Evicted = false

            assertAll("t2", {
                val onEvict = AutoCloseable { t2Evicted = true }

                val pathSlice = PathSlice(Path.of("t2/50"), 0, 50)
                cache.get(pathSlice) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(2, b1.getByte(0))

                    assertEquals(PinningCache.Stats(50L, 100L, 100L), cache.pinningCache.stats0)
                }

                Thread.sleep(100)
                assertEquals(PinningCache.Stats(0L, 150L, 100L), cache.pinningCache.stats0)
            })

            assertFalse(t1Evicted)
            assertFalse(t2Evicted)

            assertAll("t3 evicts t2/t1", {
                cache.get(PathSlice(Path.of("t3/170"), 0, 170)) { completedFuture(it to null) }.use { b1 ->
                    assertEquals(3, b1.getByte(0))
                    assertTrue(t1Evicted)

                    // definitely needs to evict t1, may or may not evict t2
                    val stats = cache.pinningCache.stats0
                    assertEquals(170, stats.pinnedBytes)
                    assertEquals(80, stats.evictableBytes + stats.freeBytes)
                }

                Thread.sleep(100)
                val stats = cache.pinningCache.stats0
                assertEquals(0, stats.pinnedBytes)
                assertEquals(250, stats.evictableBytes + stats.freeBytes)
            })
        }
    }
}