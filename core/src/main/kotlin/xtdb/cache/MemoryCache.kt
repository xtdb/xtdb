@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.sun.nio.file.ExtendedOpenOption
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.BufferLedger
import org.apache.arrow.memory.OutOfMemoryException
import xtdb.util.ReclaimingAllocationListener
import xtdb.util.closeOnCatch
import xtdb.util.maxDirectMemory
import java.io.IOException
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.io.path.fileSize

/**
 * Holds data pages in memory of its own, up to `maxSizeBytes`.
 *
 * A [get] that finds its page resident pins it on the caller's thread.
 * A miss reads the page from the local file its [Fetch] returns — past the OS page cache, where the filesystem supports
 * `O_DIRECT` — and keeps it until it's evicted.
 * Pages are evicted (S3-FIFO) only while no buffer [get] returned for them is open; if every page is held, [get]
 * throws [OutOfMemoryException].
 */
class MemoryCache @JvmOverloads internal constructor(
    al: BufferAllocator,
    maxSizeBytes: Long,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(IO_PARALLELISM, "memory-cache-io"),
    private val directIO: Boolean = true,
) : AutoCloseable {

    private val al = al.newChildAllocator("memory-cache", 0, maxSizeBytes)

    data class Slice(val offset: Long, val length: Long) {
        companion object {
            fun from(path: Path) = Slice(0, path.fileSize())
        }
    }

    internal data class PathSlice(val path: Path, val slice: Slice? = null)

    @FunctionalInterface
    fun interface Fetch {
        /**
         * @return the on-disk path, and an optional action to run once the page has been read from it
         */
        suspend operator fun invoke(k: Path): Pair<Path, AutoCloseable?>
    }

    private sealed interface Entry

    private class Loading : Entry {
        val done = CompletableDeferred<Unit>()
    }

    private class Page(val pathSlice: PathSlice, val frame: ArrowBuf, val length: Long) : Entry {
        // hits increment this without the lock: a lost increment only blurs the eviction priority
        @Volatile
        var freq = 0

        val bytes get() = frame.capacity()

        // the cache's own reference is the one the frame was allocated with;
        // a reader that has transferred the page into its own allocator owns it, leaving that reference the only one here
        val isHeld get() = frame.refCnt() > 1 || (frame.referenceManager as? BufferLedger)?.isOwningLedger == false

        /**
         * @return a buffer over the page that the caller closes, or null if the page was evicted and freed since it was found
         */
        fun retainSlice(): ArrowBuf? =
            try {
                frame.referenceManager.retain()
                frame.slice(0, length)
            } catch (_: IllegalArgumentException) {
                null
            }
    }

    private val entries = ConcurrentHashMap<PathSlice, Entry>()

    private val bounceArena = Arena.ofShared()
    private val bounces = ConcurrentLinkedQueue<MemorySegment>()

    // S3-FIFO: a page read once passes through `small`; one read again while there moves to `main`, which is a CLOCK;
    // `ghost` remembers recent evictions from `small`, so a page re-read soon after goes straight to `main`
    private val lock = Any()
    private val small = ArrayDeque<Page>()
    private val main = ArrayDeque<Page>()
    private var smallBytes = 0L
    private var mainBytes = 0L
    private val smallTargetBytes = maxSizeBytes / SMALL_QUEUE_RATIO
    private val ghost = object : LinkedHashMap<PathSlice, Unit>() {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<PathSlice, Unit>) =
            size > maxOf(MIN_GHOST_CAPACITY, small.size + main.size)
    }

    // by directory; 0 where the filesystem doesn't support O_DIRECT
    private val directBlockSizes = ConcurrentHashMap<Path, Long>()

    suspend fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf {
        val pathSlice = PathSlice(key, slice)

        while (true) {
            when (val entry = entries[pathSlice]) {
                is Page -> entry.retainSlice()?.let { buf ->
                    if (entry.freq < MAX_FREQ) entry.freq++
                    return buf
                }

                is Loading -> try {
                    entry.done.await()
                } catch (_: CancellationException) {
                    // either we were cancelled, or whoever was loading it was - in which case we load it ourselves
                    currentCoroutineContext().ensureActive()
                }

                null -> {
                    val loading = Loading()
                    if (entries.putIfAbsent(pathSlice, loading) == null) return load(pathSlice, loading, fetch)
                }
            }
        }
    }

    private suspend fun load(pathSlice: PathSlice, loading: Loading, fetch: Fetch): ArrowBuf =
        try {
            val (localPath, onRead) = fetch(pathSlice.path)

            val page = onRead.closeOnCatch { readPage(pathSlice, localPath, pathSlice.slice ?: Slice.from(localPath)) }
            try {
                onRead?.close()
            } catch (t: Throwable) {
                page.frame.close()
                throw t
            }

            val buf = page.retainSlice()!!

            synchronized(lock) {
                if (ghost.remove(pathSlice) != null) {
                    main.addLast(page); mainBytes += page.bytes
                } else {
                    small.addLast(page); smallBytes += page.bytes
                }
            }

            entries.replace(pathSlice, loading, page)
            loading.done.complete(Unit)
            buf
        } catch (t: Throwable) {
            entries.remove(pathSlice, loading)
            loading.done.completeExceptionally(t)
            throw t
        }

    private suspend fun readPage(pathSlice: PathSlice, path: Path, slice: Slice): Page =
        // allocated outside `withContext`, which discards the block's result if we're cancelled meanwhile
        allocate(slice.length).closeOnCatch { frame ->
            val dest = MemorySegment.ofAddress(frame.memoryAddress()).reinterpret(slice.length)

            withContext(ioDispatcher) {
                try {
                    openDirect(path)
                        ?.let { (ch, blockSize) -> ch.use { it.readDirect(path, slice, dest, blockSize) } }
                        ?: FileChannel.open(path, READ).use { ch -> ch.readFully(path, dest, slice.offset) }
                } catch (e: ClosedByInterruptException) {
                    throw InterruptedException(e.message)
                }
            }

            Page(pathSlice, frame, slice.length)
        }

    private fun FileChannel.readFully(path: Path, dest: MemorySegment, position: Long) {
        var done = 0L
        while (done < dest.byteSize()) {
            val bb = dest.asSlice(done, minOf(dest.byteSize() - done, MAX_BUFFERED_READ_BYTES)).asByteBuffer()
            while (bb.hasRemaining()) {
                if (read(bb, position + done + bb.position()) <= 0)
                    throw IOException("short read of $path at $position: ${done + bb.position()} of ${dest.byteSize()} bytes")
            }
            done += bb.limit()
        }
    }

    // O_DIRECT needs file positions, lengths and memory aligned to the filesystem's block size, which a page's slice and
    // its exactly-sized frame aren't - so we read aligned chunks into a bounce buffer and copy out the page's bytes
    private fun FileChannel.readDirect(path: Path, slice: Slice, dest: MemorySegment, blockSize: Long) {
        fun Long.alignDown() = this and (blockSize - 1).inv()
        fun Long.alignUp() = (this + blockSize - 1).alignDown()

        val bounce = bounces.poll() ?: bounceArena.allocate(BOUNCE_BYTES, BOUNCE_BYTES)
        try {
            val end = slice.offset + slice.length
            var filePos = slice.offset.alignDown()
            var copied = 0L

            while (copied < slice.length) {
                val bb = bounce.asSlice(0, minOf(BOUNCE_BYTES, (end - filePos).alignUp())).asByteBuffer()
                while (bb.hasRemaining()) {
                    if (read(bb, filePos + bb.position()) <= 0) break
                    // an O_DIRECT read only comes up short at the end of the file, where the next position isn't aligned
                    if (bb.position().toLong() != bb.position().toLong().alignDown()) break
                }

                val skip = slice.offset + copied - filePos
                val n = minOf(bb.position() - skip, slice.length - copied)
                if (n <= 0) throw IOException("short read of $path at ${slice.offset}: $copied of ${slice.length} bytes")

                MemorySegment.copy(bounce, skip, dest, copied, n)
                copied += n
                filePos += bb.position()
            }
        } finally {
            bounces.offer(bounce)
        }
    }

    /**
     * @return the channel and the block size its reads have to be aligned to,
     *   or null where the file's filesystem doesn't support O_DIRECT (e.g. tmpfs) or its blocks don't fit the bounce buffer
     */
    private fun openDirect(path: Path): Pair<FileChannel, Long>? {
        if (!directIO) return null

        val dir = path.toAbsolutePath().parent
        val blockSize = directBlockSizes.computeIfAbsent(dir) {
            try {
                Files.getFileStore(dir).blockSize.takeIf { it in 1..BOUNCE_BYTES && it.countOneBits() == 1 } ?: 0
            } catch (_: IOException) {
                0
            }
        }
        if (blockSize == 0L) return null

        return try {
            FileChannel.open(path, READ, ExtendedOpenOption.DIRECT) to blockSize
        } catch (_: NoSuchFileException) {
            // the buffered open says so
            null
        } catch (_: UnsupportedOperationException) {
            directBlockSizes[dir] = 0
            null
        } catch (_: IOException) {
            // e.g. tmpfs, which rejects O_DIRECT at open
            directBlockSizes[dir] = 0
            null
        }
    }

    private fun allocate(bytes: Long): ArrowBuf {
        if (al.roundingPolicy.getRoundedSize(bytes) > al.limit)
            throw OutOfMemoryException("page of $bytes bytes is larger than the memory cache (${al.limit} bytes)")

        synchronized(lock) {
            while (true) {
                try {
                    return al.buffer(bytes)
                } catch (e: OutOfMemoryException) {
                    if (evictOne() == 0L) throw e
                }
            }
        }
    }

    private fun reclaim(bytes: Long): Boolean =
        synchronized(lock) {
            var freed = 0L
            while (freed < bytes) freed += evictOne().takeIf { it > 0 } ?: break
            freed > 0
        }

    /**
     * @return the bytes it freed - 0 if every resident page is held
     */
    private fun evictOne(): Long {
        // held pages each queue has rotated past since it last made progress - once one has been all the way round, use the other
        var smallSkips = 0
        var mainSkips = 0

        // hits raise `freq` without the lock, so this bounds the scan even if they keep pace with it
        var visits = (MAX_FREQ + 2) * (small.size + main.size) + 1

        while (visits-- > 0) {
            val smallLive = small.isNotEmpty() && smallSkips < small.size
            val mainLive = main.isNotEmpty() && mainSkips < main.size

            if (smallLive && (smallBytes >= smallTargetBytes || !mainLive)) {
                val page = small.removeFirst().also { smallBytes -= it.bytes }
                when {
                    page.freq > 0 -> {
                        smallSkips = 0
                        page.freq = 0
                        main.addLast(page); mainBytes += page.bytes
                    }

                    !page.isHeld -> {
                        evict(page)
                        ghost[page.pathSlice] = Unit
                        return page.bytes
                    }

                    else -> {
                        smallSkips++
                        small.addLast(page); smallBytes += page.bytes
                    }
                }
            } else if (mainLive) {
                val page = main.removeFirst()
                when {
                    // a held page keeps its frequency: it can't be evicted, so ageing it would only count against it later
                    page.isHeld -> mainSkips++

                    page.freq > 0 -> {
                        mainSkips = 0
                        page.freq--
                    }

                    else -> {
                        mainBytes -= page.bytes
                        evict(page)
                        return page.bytes
                    }
                }
                main.addLast(page)
            } else return 0
        }

        return 0
    }

    private fun evict(page: Page) {
        entries.remove(page.pathSlice, page)
        page.frame.close()
    }

    // registered last, because another thread's failed allocation can call `reclaim` as soon as it is
    private val reclaimRegistration = (al.listener as? ReclaimingAllocationListener)?.register(::reclaim)

    override fun close() {
        reclaimRegistration?.close()

        synchronized(lock) {
            entries.clear()
            (small + main).forEach { it.frame.close() }
            small.clear(); main.clear(); smallBytes = 0; mainBytes = 0
        }

        bounceArena.close()
        al.close()
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

    internal val stats0 get() = al.let { Stats(it.allocatedMemory, it.limit - it.allocatedMemory) }

    private fun residentBytes(held: Boolean) =
        synchronized(lock) { small.sumOf { if (it.isHeld == held) it.bytes else 0 } + main.sumOf { if (it.isHeld == held) it.bytes else 0 } }

    companion object {
        private const val IO_PARALLELISM = 8
        private const val BOUNCE_BYTES = 1L shl 20
        private const val MAX_FREQ = 3
        private const val SMALL_QUEUE_RATIO = 10
        private const val MAX_BUFFERED_READ_BYTES = 1L shl 30
        private const val MIN_GHOST_CAPACITY = 16

        @JvmStatic
        fun factory() = Factory()

        internal fun MeterRegistry.registerMemoryCache(cache: MemoryCache) {
            fun registerGauge(name: String, f: MemoryCache.() -> Long) {
                Gauge.builder(name, cache) { cache.f().toDouble() }
                    .baseUnit("bytes").register(this@registerMemoryCache)
            }

            registerGauge("memory-cache.pinnedBytes") { residentBytes(held = true) }
            registerGauge("memory-cache.evictableBytes") { residentBytes(held = false) }
            registerGauge("memory-cache.freeBytes") { al.limit - al.allocatedMemory }
        }
    }
}
