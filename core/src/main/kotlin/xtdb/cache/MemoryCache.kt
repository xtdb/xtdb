@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import xtdb.cache.PinningCache.IEntry
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

data class PathSlice(val path: Path, val offset: Long? = null, val length: Long? = null) {
    constructor(path: Path) : this(path, null, null)
}

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
class MemoryCache @JvmOverloads constructor(
    private val allocator: BufferAllocator,

    @Suppress("MemberVisibilityCanBePrivate")
    val maxSizeBytes: Long,
    private val pathLoader: PathLoader = PathLoader()
) : AutoCloseable {

    internal val pinningCache = PinningCache<PathSlice, Entry>(maxSizeBytes)

    data class SectionStats(val sliceCount: Int, val weightBytes: Long, val pinned: Long, val unpinned: Long) {
        internal companion object {
            operator fun invoke(slices: List<PinningCacheMapEntry>): SectionStats {
                val size = slices.size
                val weightBytes = slices.sumOf { it.key.length ?: 0 }
                val pinned = slices.sumOf { if (it.value.get().inner.refCount.get() > 0) 1L else 0L }
                return SectionStats(size, weightBytes, pinned, size - pinned)
            }
        }
    }

    data class Stats(val metaStats: SectionStats, val dataStats: SectionStats)

    internal val stats0: Stats
        get() {
            val grouped = pinningCache.cache.asMap().entries.groupBy { it.key.path.isMetaFile }
            return Stats(SectionStats(grouped[true].orEmpty()), SectionStats(grouped[false].orEmpty()))
        }

    private val statsCache =
        Caffeine.newBuilder().expireAfterWrite(2.seconds.toJavaDuration())
            .build<Unit, Stats> { stats0 }

    val stats: Stats get() = statsCache[Unit]

    fun registerMetrics(meterName: String, registry: MeterRegistry) {
        pinningCache.registerMetrics(meterName, registry)

        fun registerGauge(name: String, baseUnit: String? = null, f: SectionStats.() -> Double) {
            Gauge.builder("$meterName.metaFiles.$name", this) { it.stats.metaStats.f() }
                .baseUnit(baseUnit).tag("type", "meta").register(registry)

            Gauge.builder("$meterName.dataFiles.$name", this) { it.stats.dataStats.f() }
                .baseUnit(baseUnit).tag("type", "data").register(registry)
        }

        registerGauge("sliceCount") { sliceCount.toDouble() }
        registerGauge("weightBytes", "bytes") { weightBytes.toDouble() }
        registerGauge("pinned") { pinned.toDouble() }
        registerGauge("unpinned") { unpinned.toDouble() }
    }

    interface PathLoader {
        fun load(path: Path): ByteBuf
        fun load(pathSlice: PathSlice): ByteBuf
        fun tryFree(bbuf: ByteBuffer) {}

        companion object {
            operator fun invoke() = object : PathLoader {
                override fun load(path: Path) =
                    try {
                        val ch = FileChannel.open(path)
                        val size = ch.size()
                        val nettyBuf = Unpooled.directBuffer(size.toInt())
                        val bbuf = nettyBuf.nioBuffer(0, size.toInt())
                        ch.read(bbuf)
                        nettyBuf

                    } catch (e: ClosedByInterruptException) {
                        throw InterruptedException(e.message)
                    }

                override fun load(pathSlice: PathSlice) =
                    try {
                        val ch = FileChannel.open(pathSlice.path)
                        val nettyBuf = Unpooled.directBuffer(pathSlice.length!!.toInt())
                        val bbuf = nettyBuf.nioBuffer(0, pathSlice.length.toInt())
                        ch.position(pathSlice.offset!!)
                        ch.read(bbuf)
                        nettyBuf

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
        operator fun invoke(k: PathSlice): CompletableFuture<Pair<PathSlice, AutoCloseable?>>
    }

    @Suppress("NAME_SHADOWING")
    fun get(k: PathSlice, fetch: Fetch): ArrowBuf {
        val entry = pinningCache.get(k) { k ->
            fetch(k).thenApplyAsync { (pathSlice, onEvict) ->
                val nettyBuf =
                    if (pathSlice.offset != null && pathSlice.length != null) {
                        pathLoader.load(pathSlice)
                    } else {
                        pathLoader.load(pathSlice.path)
                    }
                Entry(pinningCache.Entry(nettyBuf.capacity().toLong()), onEvict, nettyBuf)
            }
        }.get()!!

        val nettyBuf = entry.nettyBuf

        // create a new ArrowBuf for each request.
        // when the ref-count drops to zero, we release a ref-count in the cache.
        return allocator.wrapForeignAllocation(
            object : ForeignAllocation(nettyBuf.capacity().toLong(), nettyBuf.memoryAddress()) {
                override fun release0() {
                    pinningCache.releaseEntry(k)
                }
            })
    }

    fun invalidate(k: PathSlice) = pinningCache.invalidate(k)

    override fun close() {
        pinningCache.close()
    }
}