@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import xtdb.cache.MemoryCache.PathSlice
import xtdb.util.*
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.fileSize

private typealias FetchReqs = MutableMap<PathSlice, MutableSet<CompletableDeferred<ArrowBuf>>>

private val LOGGER = MemoryCache::class.logger

/**
 * NOTE: the allocation count metrics in the provided `allocator` WILL NOT be accurate
 *   - we allocate a buffer in here for every _usage_, and don't take shared memory into account.
 *
 * This isn't *caching* much now (see #4831) - it's more to keep track of/limit how much we've mmap'd in.
 */
class MemoryCache @JvmOverloads internal constructor(
    al: BufferAllocator,
    maxSizeBytes: Long,
    private val pathLoader: PathLoader = PathLoader(),
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {
    private val cacheAl = al.newChildAllocator("memory-cache", 0, maxSizeBytes)

    private val openSlices = ConcurrentHashMap<PathSlice, ArrowBuf>()

    // For testing
    private suspend fun yieldIfSimulation() {
        if (dispatcher != Dispatchers.IO) yield()
    }

    data class Slice(val offset: Long, val length: Long) {
        companion object {
            fun from(path: Path) = Slice(0, path.fileSize())
        }
    }

    internal data class PathSlice(val path: Path, val slice: Slice? = null) {
        constructor(path: Path, offset: Long, length: Long) : this(path, Slice(offset, length))
    }

    @FunctionalInterface
    fun interface Fetch {
        /**
         * @return a pair containing the on-disk path and an optional cleanup action
         */
        suspend operator fun invoke(k: Path): Pair<Path, AutoCloseable?>
    }

    interface PathLoader {
        fun load(path: Path, slice: Slice, arena: Arena): MemorySegment

        companion object {
            operator fun invoke() = object : PathLoader {
                override fun load(path: Path, slice: Slice, arena: Arena): MemorySegment =
                    try {
                        path.openReadableChannel().use { ch ->
                            ch.map(FileChannel.MapMode.READ_ONLY, slice.offset, slice.length, arena)
                        }
                    } catch (e: ClosedByInterruptException) {
                        throw InterruptedException(e.message)
                    }
            }
        }
    }

    private suspend fun getOpenSlice(pathSlice: PathSlice) =
        openSlices[pathSlice]?.let { buf ->
            try {
                yieldIfSimulation() // interleave between lookup and retain
                buf.also { it.referenceManager.retain() }
            } catch (_: IllegalArgumentException) {
                null
            }
        }

    private val fetchParentJob = Job()
    private val scope = CoroutineScope(SupervisorJob(fetchParentJob) + dispatcher)

    private sealed interface FetchChEvent {
        suspend fun handle(fetchReqs: FetchReqs)
    }

    private inner class FetchReq(
        val pathSlice: PathSlice, val fetch: Fetch, val res: CompletableDeferred<ArrowBuf>
    ) : FetchChEvent {
        override suspend fun handle(fetchReqs: FetchReqs) {
            val openSlice =
                runCatching { getOpenSlice(pathSlice) }
                    .onFailure { res.completeExceptionally(it) }
                    .getOrThrow()

            if (openSlice != null) {
                LOGGER.trace("FetchReq: Fast path cache hit for $pathSlice")
                res.complete(openSlice)
            } else {
                yieldIfSimulation() // interleave between openSlice check and compute
                val path = pathSlice.path
                fetchReqs.compute(pathSlice) { _, awaiters ->
                    if (awaiters != null) {
                        LOGGER.trace("FetchReq: Fetch in progress for $pathSlice - adding to ${awaiters.size} awaiter(s)")
                        awaiters.also { it.add(res) }
                    } else {
                        LOGGER.trace("FetchReq: Starting new fetch for $pathSlice")
                        val res = mutableSetOf(res)
                        scope.launch(CoroutineName("MemoryCache-Fetch-$pathSlice")) {
                            val (localPath, onEvict) = fetch(path)
                            try {
                                LOGGER.trace("FetchReq: Fetched $pathSlice to $localPath, sending FetchDone message...")
                                fetchCh.send(FetchDone(pathSlice, localPath, onEvict))
                            } catch (t: Throwable) {
                                onEvict?.close()
                                throw t
                            }
                        }
                        res
                    }
                }
            }
        }
    }

    private inner class FetchDone(
        val pathSlice: PathSlice, val localPath: Path, val onEvict: AutoCloseable?
    ) : FetchChEvent {
        override suspend fun handle(fetchReqs: FetchReqs) {
            val reqs = fetchReqs.remove(pathSlice).orEmpty()
            LOGGER.trace("FetchDone: Completing $pathSlice for ${reqs?.size ?: 0} awaiter(s)")

            try {
                val buf = try {
                    // we open up a fine-grained arena here so that we can release the memory
                    // as soon as we're done with the ArrowBuf.
                    Arena.ofShared().closeOnCatch { arena ->
                        val slice = pathSlice.slice ?: Slice.from(localPath)
                        val memSeg = pathLoader.load(localPath, slice, arena)

                        cacheAl.wrapForeignAllocation(
                            object : ForeignAllocation(memSeg.byteSize(), memSeg.address()) {
                                override fun release0() {
                                    arena.close()
                                    onEvict?.close()
                                    openSlices.computeIfPresent(pathSlice) { _, buf ->
                                        buf.takeIf { it.referenceManager.refCount > 0 }
                                    }
                                }
                            })
                    }
                } catch (t: Throwable) {
                    onEvict?.close()
                    throw t
                }

                try {
                    if (reqs.isEmpty()) {
                        LOGGER.trace("FetchDone: No awaiters for $pathSlice")
                        buf.referenceManager.release()
                    } else {
                        LOGGER.trace("FetchDone: Loaded $pathSlice into memory for ${reqs.size} awaiter(s)")
                        openSlices[pathSlice] = buf
                        yieldIfSimulation() // interleave between put and completing awaiters
                        // (size - 1) because we already have refCount 1 from allocation
                        (reqs.size - 1).takeIf { it > 0 }?.let { buf.referenceManager.retain(it) }
                        reqs.forEach {
                            if (!it.complete(buf)) buf.referenceManager.release()
                        }
                    }

                } catch (t: Throwable) {
                    buf.close()
                    throw t
                }
            } catch (t: Throwable) {
                reqs.forEach { it.completeExceptionally(t) }
            }
        }
    }

    private val fetchCh = Channel<FetchChEvent>()

    init {
        CoroutineScope(fetchParentJob + dispatcher + CoroutineName("MemoryCache FetchLoop")).launch {
            val fetchReqs: FetchReqs = mutableMapOf()

            try {
                for (req in fetchCh)
                    req.handle(fetchReqs)
            } catch (t: Throwable) {
                fetchReqs.values.flatMap { it }.forEach { it.cancel() }
                throw t
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Suppress("NAME_SHADOWING")
    suspend fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf {
        LOGGER.debug("Sending get for $key / $slice...")
        val pathSlice = PathSlice(key, slice)

        getOpenSlice(pathSlice)?.let {
            LOGGER.trace("get: Fast path cache hit for $pathSlice")
            return it
        }

        yieldIfSimulation() // interleave between cache check and fetch request

        val res = CompletableDeferred<ArrowBuf>()
        fetchCh.send(FetchReq(pathSlice, fetch, res))

        return try {
            res.await()
        } catch (e: Throwable) {
            res.cancel()
            if (res.isCompleted && !res.isCancelled) {
                val completedBuf = res.getCompleted()
                LOGGER.trace("get: Buffer was completed before cancel, releasing (refCnt=${completedBuf.refCnt()})")
                completedBuf.referenceManager.release()
            }
            throw e
        }
    }

    override fun close() {
        fetchCh.close()
        runBlocking { fetchParentJob.cancelAndJoin() }
        cacheAl.close()
    }

    /**
     * @property maxSizeRatio max size of the cache, as a proportion of the maximum direct memory of the JVM
     */
    @Serializable
    class Factory(var maxSizeBytes: Long? = null, var maxSizeRatio: Double = 0.5) {
        fun maxSizeBytes(maxSizeBytes: Long?) = apply { this.maxSizeBytes = maxSizeBytes }
        fun maxSizeRatio(maxSizeRatio: Double) = apply { this.maxSizeRatio = maxSizeRatio }

        fun open(al: BufferAllocator, meterRegistry: MeterRegistry? = null): MemoryCache {
            val maxSizeBytes = maxSizeBytes ?: (maxDirectMemory * maxSizeRatio).toLong()
            return MemoryCache(al, maxSizeBytes)
                .also { meterRegistry?.registerMemoryCache(it) }
        }
    }

    internal data class Stats(val usedBytes: Long, val freeBytes: Long)

    internal val stats0 get() = cacheAl.let { Stats(it.allocatedMemory, it.limit - it.allocatedMemory) }

    companion object {
        @JvmStatic
        fun factory() = Factory()

        internal fun MeterRegistry.registerMemoryCache(cache: MemoryCache) {
            fun registerGauge(name: String, f: MemoryCache.() -> Long) {
                Gauge.builder(name, cache) { cache.f().toDouble() }
                    .baseUnit("bytes").register(this@registerMemoryCache)
            }

            registerGauge("memory-cache.pinnedBytes") { cache.cacheAl.allocatedMemory }
            registerGauge("memory-cache.evictableBytes") { 0 }
            registerGauge("memory-cache.freeBytes") { cache.cacheAl.let { it.limit - it.allocatedMemory } }
        }
    }
}