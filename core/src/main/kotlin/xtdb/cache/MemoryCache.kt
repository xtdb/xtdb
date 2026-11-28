@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.Caffeine
import java.net.URI
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import xtdb.api.error.Fault
import xtdb.util.*
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.Future
import java.util.function.BiFunction
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

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
        // open buffers, plus one for the cache while the file is mapped.
        // zero means claimed for unmapping, and never goes back up: whoever wins the CAS to zero is the only one who unmaps.
        private val pins = AtomicInteger(1)

        @Volatile
        var lastUsed = System.nanoTime()
            private set

        val isIdle get() = pins.get() == 1

        fun tryPin(): Boolean {
            while (true) {
                val n = pins.get()
                if (n == 0) return false
                if (pins.compareAndSet(n, n + 1)) {
                    lastUsed = System.nanoTime()
                    return true
                }
            }
        }

        fun unpin() {
            pins.decrementAndGet()
        }

        fun tryClaim() = pins.compareAndSet(1, 0)

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

    private val scope = CoroutineScope(SupervisorJob() + dispatcher)

    // unbounded: Caffeine's size eviction can't see which files have open buffers, so `evictIdleFiles` evicts instead
    private val mappedFiles: AsyncCache<Path, MappedFile> = Caffeine.newBuilder().buildAsync()

    private val CompletableFuture<MappedFile>.loaded get() = takeIf { it.state() == Future.State.SUCCESS }?.resultNow()

    private suspend fun load(path: Path, fetch: Fetch): MappedFile {
        val (localPath, onEvict) = fetch(path)
        return try {
            Arena.ofShared().closeOnCatch { arena ->
                MappedFile(path, arena, pathLoader.load(localPath, arena), onEvict)
            }
        } catch (t: Throwable) {
            onEvict?.close()
            throw t
        }
    }

    private fun closed(cause: Throwable? = null) =
        Fault("memory cache is closed", "xtdb.memory-cache/closed", cause = cause)

    // removed before it fails, so that no get looks up a failed load it didn't wait on.
    // a job cancelled after completing its future still ends cancelled, and removing that future would leave its file mapped where neither `close()` nor eviction finds it.
    private fun failLoad(path: Path, future: CompletableFuture<MappedFile>, t: Throwable) {
        if (future.state() == Future.State.SUCCESS) return
        mappedFiles.asMap().remove(path, future)
        future.completeExceptionally(t)
    }

    private fun mappedFile(path: Path, fetch: Fetch): CompletableFuture<MappedFile> {
        var job: Job? = null

        val future = mappedFiles.get(path, BiFunction<Path, Executor, CompletableFuture<MappedFile>> { _, _ ->
            val future = CompletableFuture<MappedFile>()

            // lazy, so that the load runs outside Caffeine's per-key lock
            job = scope.launch(CoroutineName("MemoryCache-Fetch-$path"), start = CoroutineStart.LAZY) {
                try {
                    val file = load(path, fetch)

                    // `close()` walks the table once this job has ended, so a file loaded after it cancelled the scope is unmapped here
                    if (!isActive) {
                        file.unmap()
                        failLoad(path, future, CancellationException("memory cache is closed"))
                        return@launch
                    }

                    // held while the future completes, so a waiter whose callback `complete` runs pins the file before eviction can find it idle
                    file.tryPin()
                    try {
                        future.complete(file)
                    } finally {
                        file.unpin()
                    }
                } catch (t: Throwable) {
                    failLoad(path, future, t)
                }
            }

            future
        })

        // outside the mapping function: a job launched on a cancelled scope completes at once, and `failLoad` can't touch the table from inside its compute
        job?.let { job ->
            job.invokeOnCompletion { cause -> if (cause != null) failLoad(path, future, cause) }
            try {
                job.start()
            } catch (t: Throwable) {
                failLoad(path, future, t)
            }
        }

        return future
    }

    /**
     * Pins the loaded file in the future's completion callback.
     * Null when the file was already claimed for unmapping.
     *
     * Not `CompletableFuture.await`: that cancels the future when its waiter is cancelled, and the load is shared by every waiter.
     */
    private suspend fun CompletableFuture<MappedFile>.awaitPinned(): MappedFile? =
        suspendCancellableCoroutine { cont ->
            // the callback stays registered until the load ends, so a cancelled waiter drops its continuation from it
            val waiter = AtomicReference(cont)
            cont.invokeOnCancellation { waiter.set(null) }

            whenComplete { file, e ->
                val c = waiter.getAndSet(null) ?: return@whenComplete
                when {
                    e != null -> c.resumeWithException(e)
                    file.tryPin() -> c.resume(file) { _, f, _ -> f.unpin() }
                    else -> c.resume(null)
                }
            }
        }

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

            // a failed wrap releases from inside `openSlices.compute` on this slice, where touching `openSlices` is a recursive update
            @Volatile
            var wrapping = true

            public override fun release0() {
                if (!released.compareAndSet(false, true)) return
                if (!wrapping) openSlices.computeIfPresent(pathSlice) { _, buf ->
                    buf.takeIf { it.referenceManager.refCount > 0 }
                }
                file.unpin()
            }
        }

        if (!file.tryPin())
            throw Fault("wrapping a slice of a claimed file: ${file.path}", "xtdb.memory-cache/file-claimed", mapOf("path" to file.path.toString()))
        return try {
            cacheAl.wrapForeignAllocation(allocation).also { allocation.wrapping = false }
        } catch (t: Throwable) {
            allocation.release0()
            throw t
        }
    }

    private val evictRequested = AtomicBoolean()
    private val evicting = AtomicBoolean()

    private fun evictIdleFiles() {
        evictRequested.set(true)
        while (evictRequested.get() && evicting.compareAndSet(false, true)) {
            try {
                while (evictRequested.getAndSet(false)) evictOnce()
            } finally {
                evicting.set(false)
            }
        }
    }

    private fun evictOnce() {
        val entries = mappedFiles.asMap()
        // `lastUsed` read once per file: `tryPin` rewrites it concurrently, and a sort whose keys change under it can throw
        val idle = entries.values.mapNotNull { future -> future.loaded?.takeIf { it.isIdle }?.let { Triple(future, it, it.lastUsed) } }
        var idleBytes = idle.sumOf { (_, file) -> file.segment.byteSize() }
        var idleCount = idle.size
        val byteBudget = cacheAl.limit - cacheAl.allocatedMemory

        if (idleBytes <= byteBudget && idleCount <= MAX_IDLE_FILES) return

        for ((future, file) in idle.sortedBy { (_, _, lastUsed) -> lastUsed }) {
            if (idleBytes <= byteBudget && idleCount <= MAX_IDLE_FILES) break
            idleBytes -= file.segment.byteSize()
            idleCount--
            if (!file.tryClaim()) continue
            LOGGER.trace("evicting idle file ${file.path}")
            entries.remove(file.path, future)
            file.unmap()
        }
    }

    // XTDB's own `BufferLedger` (core/src/main/java/org/apache/arrow/memory) shadows Arrow's: its retain refuses from zero without raising the count
    private fun ArrowBuf.tryRetain() =
        try {
            also { it.referenceManager.retain() }
        } catch (_: IllegalArgumentException) {
            null
        }

    private suspend fun getOpenSlice(pathSlice: PathSlice) =
        openSlices[pathSlice]?.let { buf ->
            yieldIfSimulation()
            buf.tryRetain()
        }

    private fun openSlice(file: MappedFile, pathSlice: PathSlice, onWrap: () -> Unit): ArrowBuf =
        openSlices.compute(pathSlice) { _, existing ->
            existing?.tryRetain() ?: run { onWrap(); wrapSlice(file, pathSlice) }
        }!!

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

    internal suspend fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf {
        LOGGER.debug("get $key / $slice")
        val pathSlice = PathSlice(key, slice)

        getOpenSlice(pathSlice)?.let {
            LOGGER.trace("get: Fast path cache hit for $pathSlice")
            return it
        }

        yieldIfSimulation()

        while (true) {
            val future = mappedFile(key, fetch)
            val file = try {
                future.awaitPinned()
            } catch (t: Throwable) {
                if (future.isCompletedExceptionally && !scope.isActive) throw closed(t)
                throw t
            }

            if (file == null) {
                mappedFiles.asMap().remove(key, future)
                continue
            }

            // the load's callback can pin a file after `close()` cancelled the scope, and before `close()` walks the table
            if (!scope.isActive) {
                file.unpin()
                if (file.tryClaim()) file.unmap()
                throw closed()
            }

            var wrapped = false
            try {
                yieldIfSimulation()
                return openSlice(file, pathSlice) { wrapped = true }
            } finally {
                file.unpin()
                // after the unpin, or this get's own pin keeps its file from counting as idle
                if (wrapped) {
                    // the next wrap evicts again, and throwing here would lose the buffer this get returns
                    try {
                        evictIdleFiles()
                    } catch (e: Exception) {
                        LOGGER.warn(e, "error evicting idle files")
                    }
                }
            }
        }
    }

    override fun close() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }

        for (future in mappedFiles.asMap().values) {
            val file = future.loaded ?: continue
            if (file.tryClaim()) file.unmap() else LOGGER.warn("closing while ${file.path} is still pinned")
        }
        mappedFiles.asMap().clear()

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