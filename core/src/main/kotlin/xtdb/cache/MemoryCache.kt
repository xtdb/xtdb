@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.github.benmanes.caffeine.cache.RemovalCause
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import org.apache.arrow.memory.util.MemoryUtil
import xtdb.cache.PinningCache.IEntry
import xtdb.util.isMetaFile
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

data class PathSlice(val path: Path, val offset: Long? = null, val length: Long? = null) {
    constructor(path: Path) : this(path, null, null)
}

data class CacheSectionStats(val sliceCount: Int, val weightBytes: Long, val pinned: Long, val unpinned: Long)

data class MemoryCacheStats(
    val pinningCacheStats: Stats,
    val metaStats: CacheSectionStats,
    val dataStats: CacheSectionStats
) : Stats {
    override val pinnedBytes: Long = pinningCacheStats.pinnedBytes
    override val evictableBytes: Long = pinningCacheStats.evictableBytes
    override val freeBytes: Long = pinningCacheStats.freeBytes
}

/**
 * NOTE: the allocation count metrics in the provided `allocator` WILL NOT be accurate
 *   - we allocate a buffer in here for every _usage_, and don't take shared memory into account.
 */
class MemoryCache
@JvmOverloads constructor(
    private val allocator: BufferAllocator,

    @Suppress("MemberVisibilityCanBePrivate")
    val maxSizeBytes: Long,
    private val pathLoader: PathLoader = PathLoader()
) : AutoCloseable {
    private val pinningCache = PinningCache<PathSlice, Entry>(maxSizeBytes)

    val stats: MemoryCacheStats
        get()  {
        val grouped = pinningCache.cache.asMap().entries.groupBy { isMetaFile(it.key.path) }
        val metaSlices = grouped[true] ?: emptyList()
        val dataSlices = grouped[false] ?: emptyList()
        val metaWeightBytes = metaSlices.sumOf { it.key.length ?: 0 }
        val dataWeightBytes = dataSlices.sumOf { it.key.length ?: 0 }
        val metaPinned = metaSlices.sumOf { if (it.value.get().inner.refCount.get() > 0) 1L else 0L }
        val dataPinned = dataSlices.sumOf { if (it.value.get().inner.refCount.get() > 0) 1L else 0L }
        return MemoryCacheStats(
            pinningCache.stats,
            CacheSectionStats(metaSlices.size, metaWeightBytes, metaPinned, metaSlices.size - metaPinned),
            CacheSectionStats(dataSlices.size, dataWeightBytes, dataPinned, dataSlices.size - dataPinned)
        )
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

    private inner class Entry(
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

    fun invalidate(k: PathSlice) {
        pinningCache.invalidate(k)
    }

    override fun close() {
        pinningCache.close()
    }
}