package xtdb.storage

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.api.storage.InMemoryBucket
import xtdb.api.storage.ObjectStore
import xtdb.api.storage.PrefixedObjectStore
import xtdb.api.storage.Storage
import xtdb.arrow.IntVector
import xtdb.arrow.Relation
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.test.AllocatorResolver
import xtdb.util.asPath
import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.concurrent.thread
import kotlin.io.path.createDirectories

private class YieldingObjectStore(private val inner: ObjectStore) : ObjectStore by inner {
    override suspend fun getObject(k: Path, outPath: Path): Path {
        yield()
        return inner.getObject(k, outPath)
    }
}

private class QueueExecutor : Executor {
    val queue = LinkedBlockingQueue<Runnable>()
    override fun execute(command: Runnable) = queue.put(command)
}

@Tag("property")
@ExtendWith(AllocatorResolver::class)
class FooterCacheDeadlockSimTest : SimulationTestBase() {

    // Unix `Path.hashCode` is String-like over the bytes, so these share a bin at every table size
    private val collidingKeys = listOf("AaAa", "BBBB", "AaBB", "BBAa").map { "tbl/$it.arrow".asPath }
    private val naturalKeys = (0 until 32).map { "tbl/key-$it.arrow".asPath }

    private fun ObjectStore.writeArrowFiles(keys: List<Path>, al: BufferAllocator, diskPath: Path) =
        MemoryCache.Factory().open(al).use { memoryCache ->
            RemoteBufferPool(al, this, memoryCache, DiskCache(diskPath.createDirectories(), 1L shl 30), null, 0, "xtdb", 0)
                .use { pool ->
                    for (k in keys) IntVector.open(al, "foo", false).use { v ->
                        v.writeInt(1)
                        pool.openArrowWriter(k, Relation(al, listOf(v), 1)).use { w -> w.writePage(); w.endSync() }
                    }
                }
        }

    private fun simulate(keys: List<Path>, diskPath: Path, al: BufferAllocator) {
        val bucket = PrefixedObjectStore("root".asPath, InMemoryBucket())
        bucket.writeArrowFiles(keys, al, diskPath.resolve("seed"))

        val worker = QueueExecutor()
        val workerDispatcher = worker.asCoroutineDispatcher()

        MemoryCache.Factory().open(al).use { memoryCache ->
            RemoteBufferPool(
                al, YieldingObjectStore(bucket), memoryCache,
                DiskCache(diskPath.resolve("test").createDirectories(), 1L shl 30, workerDispatcher),
                null, 0, "xtdb", 0,
                workerDispatcher,
                Storage.arrowFooterCache(1, workerDispatcher)
            ).use { pool ->
                val callerKeys = List(16) { keys[rand.nextInt(keys.size)] }
                val done = CountDownLatch(callerKeys.size)
                val failures = ConcurrentLinkedQueue<Throwable>()

                val scheduler = thread(isDaemon = true) {
                    var started = 0
                    while (done.count > 0L) {
                        val options = buildList {
                            if (started < callerKeys.size) add(0)
                            if (worker.queue.isNotEmpty()) add(1)
                        }
                        if (options.isEmpty()) {
                            Thread.onSpinWait(); continue
                        }
                        when (options[rand.nextInt(options.size)]) {
                            0 -> {
                                val k = callerKeys[started++]
                                thread(isDaemon = true) {
                                    try {
                                        runBlocking { pool.getFooter(k) }
                                    } catch (e: Throwable) {
                                        failures.add(e)
                                    } finally {
                                        done.countDown()
                                    }
                                }
                            }

                            1 -> worker.queue.poll()?.run()
                        }
                    }
                }

                val completed = done.await(1, SECONDS)

                // a stuck schedule wedges the scheduler inside a task - running the rest from here frees it, so the pool can close
                if (!completed) {
                    val deadline = System.nanoTime() + SECONDS.toNanos(10)
                    while (done.count > 0L && System.nanoTime() < deadline) {
                        worker.queue.poll()?.run() ?: Thread.onSpinWait()
                    }
                }
                scheduler.join(SECONDS.toMillis(1))

                assertTrue(completed, "footer loads stuck (seed=$currentSeed)")
                assertEquals(emptyList<Throwable>(), failures.toList())
            }
        }
    }

    @RepeatableSimulationTest
    fun `footer loads of keys sharing a bin complete`(@TempDir diskPath: Path, al: BufferAllocator) =
        simulate(collidingKeys, diskPath, al)

    @RepeatableSimulationTest
    fun `footer loads of natural keys complete`(@TempDir diskPath: Path, al: BufferAllocator) =
        simulate(naturalKeys, diskPath, al)
}
