package xtdb.cache

import com.github.benmanes.caffeine.cache.RemovalCause
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger

class PinningCacheTest {

    private class TestEntry(override val weight: Long, val evictions: MutableCollection<String>) : PinningCache.IEntry<String> {
        override val refCount = AtomicInteger(0)

        override fun onEvict(k: String, reason: RemovalCause) {
            evictions.add(k)
        }
    }

    private fun <T> withCommonPoolStarved(f: () -> T): T {
        val pool = ForkJoinPool.commonPool()
        val workersBlocked = CountDownLatch(pool.parallelism)
        val unblock = CountDownLatch(1)

        repeat(pool.parallelism) {
            pool.execute {
                workersBlocked.countDown()
                unblock.await()
            }
        }

        try {
            check(workersBlocked.await(10, SECONDS)) { "common pool workers didn't all start" }
            return f()
        } finally {
            unblock.countDown()
        }
    }

    private fun awaitUntil(f: () -> Boolean) {
        val deadline = System.nanoTime() + SECONDS.toNanos(2)
        while (!f() && System.nanoTime() < deadline) Thread.sleep(10)
    }

    @Test
    fun `releasing the last pin evicts an over-size cache without the common pool`() {
        val evictions = ConcurrentLinkedQueue<String>()

        PinningCache<String, TestEntry>(maxSizeBytes = 10).use { cache ->
            withCommonPoolStarved {
                for (k in listOf("a", "b", "c"))
                    cache.get(k) { completedFuture(TestEntry(4, evictions)) }.join()

                for ((k, stillPinned) in listOf("a" to 8L, "b" to 4L, "c" to 0L)) {
                    cache.releaseEntry(k)
                    awaitUntil { cache.stats0.pinnedBytes == stillPinned }
                }

                awaitUntil { cache.cache.asMap().size <= 2 }

                assertEquals(setOf("b", "c"), cache.cache.asMap().keys)
                assertEquals(listOf("a"), evictions.toList())
            }
        }
    }
}
