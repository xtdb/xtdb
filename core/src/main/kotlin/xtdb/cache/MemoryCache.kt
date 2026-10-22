@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.github.benmanes.caffeine.cache.AsyncCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import org.apache.arrow.memory.OutOfMemoryException
import xtdb.util.*
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

private val LOGGER = MemoryCache::class.logger

private const val CACHE_HOLD = 1
private const val SLICE = 2

/**
 * Maps each file once, and hands out slices of that mapping.
 *
 * `maxSizeBytes` bounds two things separately:
 * - the bytes of the distinct slices in use at once - a `get` that would exceed it throws [OutOfMemoryException];
 *   a slice in use by several callers counts once;
 * - the bytes of the files kept mapped while no slice of them is in use - beyond it, the cache unmaps files.
 */
class MemoryCache @JvmOverloads internal constructor(
    al: BufferAllocator,
    private val maxSizeBytes: Long,
    private val pathLoader: PathLoader = PathLoader(),
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {
    private val cacheAl = al.newChildAllocator("memory-cache", 0, Long.MAX_VALUE)

    // For testing
    private suspend fun yieldIfSimulation() {
        if (dispatcher != Dispatchers.IO) yield()
    }

    data class Slice(val offset: Long, val length: Long)

    @FunctionalInterface
    fun interface Fetch {
        /**
         * Called at most once per mapping of a file.
         *
         * @return a pair containing the on-disk path and an optional cleanup action, closed when the file is unmapped
         */
        suspend operator fun invoke(k: Path): Pair<Path, AutoCloseable?>
    }

    interface PathLoader {
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

    private val sliceBytes = AtomicLong()

    private fun reserveBytes(length: Long) {
        while (true) {
            val used = sliceBytes.get()
            if (used + length > maxSizeBytes)
                throw OutOfMemoryException(
                    "Memory cache: unable to take a slice of $length bytes - $used of $maxSizeBytes bytes are in use"
                )

            if (sliceBytes.compareAndSet(used, used + length)) return
        }
    }

    /**
     * [state] is what unmaps the file: bit 0 is the cache's hold, the rest count live slices.
     * It reaches zero once, and [tryRetain] fails rather than resurrect it (#5013).
     */
    private inner class MappedFile(
        val path: Path,
        private val arena: Arena,
        private val segment: MemorySegment,
        private val onEvict: AutoCloseable?,
    ) {
        val size get() = segment.byteSize()

        private val state = AtomicInteger(CACHE_HOLD + SLICE)

        private val sliceCallers = ConcurrentHashMap<Slice, Int>()

        val weight get() = if (state.get() >= SLICE) 0 else size.coerceIn(1, Int.MAX_VALUE.toLong()).toInt()

        fun tryRetain(): Boolean {
            while (true) {
                val s = state.get()
                if (s == 0) return false
                if (state.compareAndSet(s, s + SLICE)) {
                    if (s < SLICE) reweigh(this)
                    return true
                }
            }
        }

        fun releaseSlice() {
            val s = state.addAndGet(-SLICE)
            if (s == 0) unmap()
            else if (s < SLICE) reweigh(this)
        }

        fun dropCacheHold() {
            while (true) {
                val s = state.get()
                if (s and CACHE_HOLD == 0) return
                if (state.compareAndSet(s, s - CACHE_HOLD)) {
                    if (s == CACHE_HOLD) unmap()
                    return
                }
            }
        }

        private fun releaseBytes(slice: Slice) {
            sliceCallers.compute(slice) { _, n ->
                if (n == 1) null.also { sliceBytes.addAndGet(-slice.length) } else n!! - 1
            }
        }

        fun unmap() {
            try {
                arena.close()
            } finally {
                onEvict?.close()
            }
        }

        /**
         * Takes over a slice the caller already holds, which the returned buffer releases.
         *
         * `release0` runs under the allocation's ledger lock, so nothing it reaches may take a ledger lock.
         */
        fun slice(range: Slice?): ArrowBuf {
            val slice = range ?: Slice(0, size)
            val (offset, length) = slice

            try {
                require(offset in 0..size && length in 0..(size - offset)) {
                    "slice ($offset, $length) out of bounds of $path ($size bytes)"
                }

                sliceCallers.compute(slice) { _, n -> if (n == null) reserveBytes(length); (n ?: 0) + 1 }
            } catch (t: Throwable) {
                releaseSlice()
                throw t
            }

            // Arrow itself calls `release0` if `wrapForeignAllocation` fails after accepting the allocation
            val allocation = object : ForeignAllocation(length, segment.address() + offset) {
                private val released = AtomicBoolean()

                public override fun release0() {
                    if (released.compareAndSet(false, true)) {
                        releaseBytes(slice)
                        releaseSlice()
                    }
                }
            }

            return try {
                cacheAl.wrapForeignAllocation(allocation)
            } catch (t: Throwable) {
                allocation.release0()
                throw t
            }
        }
    }

    // an inline executor keeps eviction deterministic under the simulation's dispatcher
    private val files: AsyncCache<Path, MappedFile> =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .maximumWeight(maxSizeBytes)
            .weigher<Path, MappedFile> { _, file -> file.weight }
            .removalListener<Path, MappedFile> { _, file, cause ->
                if (cause != RemovalCause.REPLACED) file?.dropCacheHold()
            }
            .buildAsync()

    private val eviction = files.synchronous().policy().eviction().get()

    // Caffeine weighs an entry when it's written, so a file that gains or loses its last slice is written back
    private fun reweigh(file: MappedFile) {
        files.asMap().computeIfPresent(file.path) { _, fut ->
            if (fut.isDone && !fut.isCompletedExceptionally && fut.getNow(null) === file) fut.thenApply { it } else fut
        }
    }

    private val scopeJob = SupervisorJob()
    private val scope = CoroutineScope(scopeJob + dispatcher)

    // MUST NOT touch [files]: a test dispatcher runs this inside the cache's own compute
    // not `scope.future`, which discards a mapping that completes after the scope is cancelled
    private fun mapFile(path: Path, fetch: Fetch): CompletableFuture<MappedFile> {
        val fut = CompletableFuture<MappedFile>()

        scope.launch(CoroutineName("MemoryCache-Fetch-$path")) {
            try {
                LOGGER.trace("Fetching $path")
                val (localPath, onEvict) = fetch(path)
                val file = try {
                    yieldIfSimulation()
                    Arena.ofShared().closeOnCatch { arena ->
                        MappedFile(path, arena, pathLoader.load(localPath, arena), onEvict)
                    }
                } catch (t: Throwable) {
                    onEvict?.close()
                    throw t
                }

                fut.complete(file)
            } catch (t: Throwable) {
                fut.completeExceptionally(t)
            }
        }.invokeOnCompletion { t -> if (t != null) fut.completeExceptionally(t) }

        return fut
    }

    /**
     * Stops keeping the files under [prefix] mapped once their slices are released, so that a later [get] maps them afresh.
     * Call it when the files under a prefix may be replaced - when the database that owns them closes, say.
     */
    fun invalidate(prefix: Path) {
        files.synchronous().invalidateAll(files.asMap().keys.filter { it.startsWith(prefix) })
    }

    private fun Throwable.unwrapCompletion() = if (this is CompletionException) cause ?: this else this

    // waiters share the future, so cancelling one MUST NOT cancel it - hence not CompletableFuture.await
    private suspend fun CompletableFuture<MappedFile>.awaitShared(): MappedFile =
        suspendCancellableCoroutine { cont ->
            whenComplete { file, ex ->
                if (ex != null) cont.resumeWithException(ex.unwrapCompletion()) else cont.resume(file)
            }
        }

    // as above, but the file carries the mapping request's slice, which a cancelled wait hands back
    private suspend fun CompletableFuture<MappedFile>.awaitMapped(): MappedFile =
        suspendCancellableCoroutine { cont ->
            whenComplete { file, ex ->
                if (ex != null) cont.resumeWithException(ex.unwrapCompletion())
                else cont.resume(file) { _, _, _ -> file.releaseSlice() }
            }
        }

    /**
     * Returns [slice] of the file at [key] - the whole file if `null` - mapping the file first if it isn't mapped.
     *
     * The caller owns the returned buffer and MUST close it; the file stays mapped at least until every buffer over it is closed.
     * [fetch] runs only to map the file, once per mapping however many callers wait on it, and a failure in it is thrown to each of them.
     *
     * @throws OutOfMemoryException if no other caller holds this slice and its bytes would take the slices in use past `maxSizeBytes`
     */
    suspend fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf {
        LOGGER.debug("Sending get for $key / $slice...")

        while (true) {
            yieldIfSimulation()

            var mapping = false
            val fut = files.get(key) { k, _ -> mapping = true; mapFile(k, fetch) }

            val file =
                if (mapping) fut.awaitMapped()
                else {
                    val file = fut.awaitShared()
                    yieldIfSimulation()
                    if (!file.tryRetain()) continue
                    file
                }

            return file.slice(slice)
        }
    }

    override fun close() {
        runBlocking { scopeJob.cancelAndJoin() }
        files.synchronous().invalidateAll()
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

    internal val stats0 get() = sliceBytes.get().let { Stats(it, maxSizeBytes - it) }

    companion object {
        @JvmStatic
        fun factory() = Factory()

        internal fun MeterRegistry.registerMemoryCache(cache: MemoryCache) {
            fun registerGauge(name: String, f: MemoryCache.() -> Long) {
                Gauge.builder(name, cache) { cache.f().toDouble() }
                    .baseUnit("bytes").register(this@registerMemoryCache)
            }

            registerGauge("memory-cache.pinnedBytes") { sliceBytes.get() }
            registerGauge("memory-cache.evictableBytes") { eviction.weightedSize().orElse(0) }
            registerGauge("memory-cache.freeBytes") { maxSizeBytes - sliceBytes.get() }
        }
    }
}
