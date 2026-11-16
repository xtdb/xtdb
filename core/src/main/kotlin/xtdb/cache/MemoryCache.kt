@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import java.net.URI
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import xtdb.api.error.Fault
import xtdb.cache.MemoryCache.PathSlice
import xtdb.util.*
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

private class Awaiter(val pathSlice: PathSlice, val res: CompletableDeferred<ArrowBuf>)

private typealias FetchReqs = MutableMap<Path, MutableList<Awaiter>>

private val LOGGER = MemoryCache::class.logger

// each mapped file is a VMA, and vm.max_map_count defaults to 65530
internal const val MAX_IDLE_FILES = 4096

/**
 * NOTE: the allocation count metrics in the provided `allocator` WILL NOT be accurate
 *   - we allocate a buffer in here for every _usage_, and don't take shared memory into account.
 *
 * Each file is mapped once, and its pages are handed out as buffers over that mapping.
 * The allocator's limit bounds the bytes of pages in use; a file with none in use stays mapped while it fits in what's left of that limit.
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

    data class Slice(val offset: Long, val length: Long)

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
        /**
         * Maps the whole of the file at [path], read-only, into [arena].
         */
        fun load(path: Path, arena: Arena): MemorySegment

        companion object {
            operator fun invoke() = object : PathLoader {
                override fun load(path: Path, arena: Arena): MemorySegment =
                    try {
                        path.openReadableChannel().use { ch ->
                            ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena)
                        }
                    } catch (e: ClosedByInterruptException) {
                        throw InterruptedException(e.message)
                    }
            }
        }
    }

    private class MappedFile(val path: Path, val arena: Arena, val segment: MemorySegment, val onEvict: AutoCloseable?) {
        // incremented only on the fetch actor, decremented by `release0` on any thread:
        // so the actor, reading zero, knows no slice can come back to life, and may unmap.
        val liveSlices = AtomicInteger()

        val isIdle get() = liveSlices.get() == 0

        fun unmap() {
            try {
                try {
                    arena.close()
                } finally {
                    onEvict?.close()
                }
            } catch (t: Throwable) {
                LOGGER.warn(t, "error unmapping $path")
            }
        }
    }

    // owned by the fetch actor, and by close() once the actor is joined; iterates least-recently-used first
    private val mappedFiles = LinkedHashMap<Path, MappedFile>(16, 0.75f, true)

    private fun wrapSlice(file: MappedFile, pathSlice: PathSlice): ArrowBuf {
        val fileSize = file.segment.byteSize()
        val slice = pathSlice.slice ?: Slice(0, fileSize)
        if (slice.offset < 0 || slice.length < 0 || slice.offset > fileSize || slice.length > fileSize - slice.offset)
            throw Fault(
                "Slice out of bounds for ${file.path}: $slice, file is $fileSize bytes",
                "xtdb.memory-cache/slice-out-of-bounds",
                mapOf("path" to file.path.toString(), "offset" to slice.offset, "length" to slice.length, "file-size" to fileSize)
            )

        val allocation = object : ForeignAllocation(slice.length, file.segment.address() + slice.offset) {
            // Arrow 19's `wrapForeignAllocation` calls `release0` itself when it fails after reserving the bytes, but not when it fails before.
            private val released = AtomicBoolean()

            public override fun release0() {
                if (!released.compareAndSet(false, true)) return
                openSlices.computeIfPresent(pathSlice) { _, buf ->
                    buf.takeIf { it.referenceManager.refCount > 0 }
                }
                file.liveSlices.decrementAndGet()
            }
        }

        file.liveSlices.incrementAndGet()
        return try {
            cacheAl.wrapForeignAllocation(allocation)
        } catch (t: Throwable) {
            allocation.release0()
            throw t
        }
    }

    private suspend fun serve(file: MappedFile, pathSlice: PathSlice, reqs: List<CompletableDeferred<ArrowBuf>>) {
        try {
            val buf = wrapSlice(file, pathSlice)

            try {
                openSlices[pathSlice] = buf
                yieldIfSimulation() // interleave between put and completing awaiters
                // (size - 1) because we already have refCount 1 from allocation
                (reqs.size - 1).takeIf { it > 0 }?.let { buf.referenceManager.retain(it) }
                reqs.forEach {
                    if (!it.complete(buf)) buf.referenceManager.release()
                }
            } catch (t: Throwable) {
                buf.close()
                throw t
            }
        } catch (t: Throwable) {
            reqs.forEach { it.completeExceptionally(t) }
        }
    }

    private fun evictIdleFiles() {
        val idleFiles = mappedFiles.values.filter { it.isIdle }
        var idleBytes = idleFiles.sumOf { it.segment.byteSize() }
        var idleCount = idleFiles.size
        val byteBudget = cacheAl.limit - cacheAl.allocatedMemory

        for (file in idleFiles) {
            if (idleBytes <= byteBudget && idleCount <= MAX_IDLE_FILES) break
            LOGGER.trace("evicting idle file ${file.path}")
            mappedFiles.remove(file.path)
            idleBytes -= file.segment.byteSize()
            idleCount--
            file.unmap()
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

        fun fail(cause: Throwable)
    }

    private inner class FetchReq(
        val pathSlice: PathSlice, val fetch: Fetch, val res: CompletableDeferred<ArrowBuf>
    ) : FetchChEvent {
        override suspend fun handle(fetchReqs: FetchReqs) {
            val openSlice = getOpenSlice(pathSlice)

            if (openSlice != null) {
                LOGGER.trace("FetchReq: Fast path cache hit for $pathSlice")
                if (!res.complete(openSlice)) openSlice.referenceManager.release()
                return
            }

            yieldIfSimulation() // interleave between openSlice check and compute
            val path = pathSlice.path

            val file = mappedFiles[path]
            if (file != null) {
                LOGGER.trace("FetchReq: $path already mapped - serving $pathSlice")
                serve(file, pathSlice, listOf(res))
                evictIdleFiles()
                return
            }

            fetchReqs.compute(path) { _, awaiters ->
                if (awaiters != null) {
                    LOGGER.trace("FetchReq: Fetch in progress for $path - adding to ${awaiters.size} awaiter(s)")
                    awaiters.also { it.add(Awaiter(pathSlice, res)) }
                } else {
                    LOGGER.trace("FetchReq: Starting new fetch for $path")
                    scope.launch(CoroutineName("MemoryCache-Fetch-$path")) {
                        val ev = try {
                            val (localPath, onEvict) = fetch(path)
                            LOGGER.trace("FetchReq: Fetched $path to $localPath, sending FetchDone message...")
                            FetchDone(path, localPath, onEvict)
                        } catch (t: Throwable) {
                            FetchFailed(path, t)
                        }

                        try {
                            withContext(NonCancellable) { fetchCh.send(ev) }
                        } catch (t: Throwable) {
                            ev.fail(t)
                        }
                    }
                    mutableListOf(Awaiter(pathSlice, res))
                }
            }
        }

        override fun fail(cause: Throwable) {
            res.completeExceptionally(cause)
        }
    }

    private inner class FetchFailed(
        val path: Path, val cause: Throwable
    ) : FetchChEvent {
        override suspend fun handle(fetchReqs: FetchReqs) {
            fetchReqs.remove(path).orEmpty().forEach { it.res.completeExceptionally(cause) }
        }

        override fun fail(cause: Throwable) = Unit
    }

    private inner class FetchDone(
        val path: Path, val localPath: Path, val onEvict: AutoCloseable?
    ) : FetchChEvent {
        override suspend fun handle(fetchReqs: FetchReqs) {
            val awaiters = fetchReqs.remove(path).orEmpty()
            LOGGER.trace("FetchDone: Completing $path for ${awaiters.size} awaiter(s)")

            try {
                val file = try {
                    Arena.ofShared().closeOnCatch { arena ->
                        MappedFile(path, arena, pathLoader.load(localPath, arena), onEvict)
                    }
                } catch (t: Throwable) {
                    onEvict?.close()
                    throw t
                }

                mappedFiles[path] = file

                awaiters.groupBy({ it.pathSlice }, { it.res })
                    .forEach { (pathSlice, reqs) -> serve(file, pathSlice, reqs) }

                evictIdleFiles()
            } catch (t: Throwable) {
                awaiters.forEach { it.res.completeExceptionally(t) }
            }
        }

        override fun fail(cause: Throwable) {
            onEvict?.close()
        }
    }

    private val fetchCh = Channel<FetchChEvent>()

    init {
        CoroutineScope(fetchParentJob + dispatcher + CoroutineName("MemoryCache FetchLoop")).launch {
            val fetchReqs: FetchReqs = mutableMapOf()

            val cause = try {
                for (ev in fetchCh) {
                    try {
                        ev.handle(fetchReqs)
                    } catch (t: Throwable) {
                        ev.fail(t)
                        throw t
                    }
                }
                null
            } catch (t: Throwable) {
                t
            }

            val failure = cause ?: CancellationException("MemoryCache closed")
            fetchCh.close(cause)
            withContext(NonCancellable) {
                while (true) {
                    val ev = fetchCh.receiveCatching().getOrNull() ?: break
                    ev.fail(failure)
                }
            }
            fetchReqs.values.flatten().forEach { it.res.completeExceptionally(failure) }

            if (cause != null) throw cause
        }
    }

    /**
     * A view of this cache whose keys are namespaced under [prefix].
     *
     * The cache is shared by every buffer pool on the node; each pool reads through its own [ForDatabase],
     * passing keys relative to its store — [get]'s [Fetch] is handed the same relative key.
     */
    inner class ForDatabase internal constructor(private val prefix: Path) {
        suspend fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf =
            this@MemoryCache.get(prefix.resolve(key), slice) { cacheKey -> fetch(prefix.relativize(cacheKey)) }
    }

    /**
     * @param storageUri where the database's objects live, including its storage root — see [xtdb.api.storage.ObjectStore.location].
     *   Null gets a namespace of its own, never shared with another open.
     */
    fun openForDatabase(storageUri: URI?) = ForDatabase(cachePrefix(storageUri))

    @OptIn(ExperimentalCoroutinesApi::class)
    @Suppress("NAME_SHADOWING")
    internal suspend fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf {
        LOGGER.debug("Sending get for $key / $slice...")
        val pathSlice = PathSlice(key, slice)

        getOpenSlice(pathSlice)?.let {
            LOGGER.trace("get: Fast path cache hit for $pathSlice")
            return it
        }

        yieldIfSimulation() // interleave between cache check and fetch request

        val res = CompletableDeferred<ArrowBuf>()

        return try {
            fetchCh.send(FetchReq(pathSlice, fetch, res))
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

        for (file in mappedFiles.values) {
            if (file.isIdle) file.unmap() else LOGGER.warn("closing with live slices over ${file.path}")
        }
        mappedFiles.clear()

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