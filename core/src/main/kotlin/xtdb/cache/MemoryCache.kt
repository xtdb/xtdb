@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocatorL
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import xtdb.cache.MemoryCache.PathSlice
import xtdb.cache.PinningCache.IEntry
import xtdb.util.maxDirectMemory
import xtdb.util.openReadableChannel
import java.nio.channels.ClosedByInterruptException
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import kotlin.io.path.fileSize
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private typealias PinningCacheMapEntry = Map.Entry<PathSlice, CompletableFuture<MemoryCache.Entry>>

private val Path.isMetaFile: Boolean
    get() {
        val regex = Regex(""".*meta/.*\.arrow$""")
        return regex.containsMatchIn(this.toString())
    }

/**
 * NOTE: the allocation count metrics in the provided `allocator` WILL NOT be accurate
 *   - we allocate a buffer in here for every _usage_, and don't take shared memory into account.
 */
class MemoryCache @JvmOverloads internal constructor(
    private val al: BufferAllocator,
    maxSizeBytes: Long,
    private val pathLoader: PathLoader = PathLoader()
) : AutoCloseable {

    data class Slice(val offset: Long, val length: Long) {
        companion object {
            fun from(path: Path) = Slice(0, path.fileSize())
        }
    }

    internal data class PathSlice(val path: Path, val slice: Slice? = null) {
        constructor(path: Path, offset: Long, length: Long) : this(path, Slice(offset, length))
    }

    /**
     * @property maxSizeRatio max size of the cache, as a proportion of the maximum direct memory of the JVM
     */
    @Serializable
    class Factory(var maxSizeBytes: Long? = null, var maxSizeRatio: Double = 0.5) {
        fun maxSizeBytes(maxSizeBytes: Long?) = apply { this.maxSizeBytes = maxSizeBytes }
        fun maxSizeRatio(maxSizeRatio: Double) = apply { this.maxSizeRatio = maxSizeRatio }

        fun open(al: BufferAllocator, meterRegistry: MeterRegistry? = null) =
            MemoryCache(al, maxSizeBytes ?: (maxDirectMemory * maxSizeRatio).toLong())
                .also { meterRegistry?.registerMemoryCache(it) }
    }

    internal val pinningCache = PinningCache<PathSlice, Entry>(maxSizeBytes)

    data class SectionStats(val sliceCount: Int, val weightBytes: Long, val pinned: Long, val unpinned: Long) {
        internal companion object {
            operator fun invoke(slices: List<PinningCacheMapEntry>): SectionStats {
                val size = slices.size
                val weightBytes = slices.sumOf { it.key.slice?.length ?: 0 }
                val pinned = slices.sumOf { if (it.value.get().inner.refCount.get() > 0) 1L else 0L }
                return SectionStats(size, weightBytes, pinned, size - pinned)
            }
        }
    }

    data class Stats(val metaStats: SectionStats, val dataStats: SectionStats)

    private val stats0: Stats
        get() {
            val grouped = pinningCache.cache.asMap().entries.groupBy { it.key.path.isMetaFile }
            return Stats(SectionStats(grouped[true].orEmpty()), SectionStats(grouped[false].orEmpty()))
        }

    private val statsCache =
        Caffeine.newBuilder().expireAfterWrite(2.seconds.toJavaDuration())
            .build<Unit, Stats> { stats0 }

    val stats: Stats get() = statsCache[Unit]

    interface PathLoader {
        fun load(path: Path, slice: Slice): ByteBuf

        companion object {
            operator fun invoke() = object : PathLoader {
                private val pool = PooledByteBufAllocatorL()

                override fun load(path: Path, slice: Slice) =
                    try {
                        path.openReadableChannel().use { ch ->
                            val nettyBuf = pool.allocate(slice.length)
                            val bbuf = nettyBuf.nioBuffer(0, slice.length.toInt())
                            ch.position(slice.offset)
                            ch.read(bbuf)
                            nettyBuf
                        }
                    } catch (e: ClosedByInterruptException) {
                        throw InterruptedException(e.message)
                    }
            }
        }
    }

    internal inner class Entry(
        val inner: IEntry<PathSlice>,
        val onEvict: AutoCloseable?,
        val nettyBuf: ByteBuf
    ) : IEntry<PathSlice> by inner {
        override fun onEvict(k: PathSlice, reason: RemovalCause) {
            nettyBuf.release()
            onEvict?.close()
        }
    }

    @FunctionalInterface
    fun interface Fetch {
        /**
         * @return a pair containing the on-disk path and an optional cleanup action
         */
        operator fun invoke(k: Path): CompletableFuture<Pair<Path, AutoCloseable?>>
    }

    @Suppress("NAME_SHADOWING")
    fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf {
        val cacheKey = PathSlice(key, slice)
        val entry = pinningCache.get(cacheKey) { k ->
            fetch(k.path).thenApplyAsync { (path, onEvict) ->
                val nettyBuf = pathLoader.load(path, k.slice ?: Slice.from(path))
                Entry(pinningCache.Entry(nettyBuf.capacity().toLong()), onEvict, nettyBuf)
            }
        }.get()!!

        val nettyBuf = entry.nettyBuf

        // create a new ArrowBuf for each request.
        // when the ref-count drops to zero, we release a ref-count in the cache.
        return al.wrapForeignAllocation(
            object : ForeignAllocation(nettyBuf.capacity().toLong(), nettyBuf.memoryAddress()) {
                override fun release0() = pinningCache.releaseEntry(cacheKey)
            })
    }

    internal fun invalidate(key: Path, slice: Slice? = null) = pinningCache.invalidate(PathSlice(key, slice))

    override fun close() {
        pinningCache.close()
    }

    companion object {
        @JvmStatic
        fun factory() = Factory()

        internal fun MeterRegistry.registerMemoryCache(cache: MemoryCache) {
            cache.pinningCache.registerMetrics("memory-cache", this)

            fun registerGauge(name: String, baseUnit: String? = null, f: SectionStats.() -> Double) {
                Gauge.builder("memory-cache.metaFiles.$name", cache) { it.stats.metaStats.f() }
                    .baseUnit(baseUnit).tag("type", "meta").register(this)

                Gauge.builder("memory-cache.dataFiles.$name", cache) { it.stats.dataStats.f() }
                    .baseUnit(baseUnit).tag("type", "data").register(this)
            }

            registerGauge("sliceCount") { sliceCount.toDouble() }
            registerGauge("weightBytes", "bytes") { weightBytes.toDouble() }
            registerGauge("pinned") { pinned.toDouble() }
            registerGauge("unpinned") { unpinned.toDouble() }
        }
    }
}
