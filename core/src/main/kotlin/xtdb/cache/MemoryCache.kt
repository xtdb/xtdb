@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.github.benmanes.caffeine.cache.RemovalCause
import com.sun.nio.file.ExtendedOpenOption
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import org.apache.arrow.memory.util.MemoryUtil
import xtdb.cache.PinningCache.IEntry
import xtdb.util.isMetaFile
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
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
        fun load(path: Path): Pair<ByteBuffer, Arena>
        fun load(pathSlice: PathSlice): Pair<ByteBuffer, Arena>

        companion object {

            private val BLOCK_SIZE = 4096;

            private fun multipleOfBlockSize(size: Long): Int {
                return (((size + BLOCK_SIZE - 1) / BLOCK_SIZE ) * BLOCK_SIZE).toInt()
            }

            private fun multipleOfBlockSizeLower(size: Long): Long {
                val res = multipleOfBlockSize(size)
                return if (res > size) (res - BLOCK_SIZE).toLong() else res.toLong()
            }


            fun getAlignedMemorySegment(arena: Arena , size: Int, alignment: Int): MemorySegment {
                val segment = arena.allocate(size.toLong(), alignment.toLong())
                return segment
            }


            operator fun invoke() = object : PathLoader {
                override fun load(path: Path): Pair<ByteBuffer, Arena> {
                    val arena = Arena.ofShared()
                    try {

                        val ch = FileChannel.open(path, setOf(StandardOpenOption.READ, ExtendedOpenOption.DIRECT))
                        val size = ch.size()
                        val sizeUpper = multipleOfBlockSize(size)

                        val segment = getAlignedMemorySegment(arena, sizeUpper, BLOCK_SIZE)
                        val bbuf = segment.asByteBuffer()
                        ch.read(bbuf)
                        bbuf.flip()

                        return bbuf to arena
                    } catch (e: ClosedByInterruptException) {
                        arena.close()
                        throw InterruptedException(e.message)
                    } catch (e : Exception) {
                        arena.close()
                        throw e
                    }
                }

                override fun load(pathSlice: PathSlice) : Pair<ByteBuffer, Arena> {
                    val arena = Arena.ofShared()
                    try {
                        require(pathSlice.length != null && pathSlice.offset != null)
                        val ch =
                            FileChannel.open(pathSlice.path, setOf(StandardOpenOption.READ, ExtendedOpenOption.DIRECT))

                        val lower = multipleOfBlockSizeLower(pathSlice.offset)
                        val diff = pathSlice.offset - lower
                        val sizeUpper = multipleOfBlockSize(pathSlice.length + diff)

                        val segment = getAlignedMemorySegment(arena, sizeUpper, BLOCK_SIZE)
                        val bbuf = segment.asByteBuffer()
                        ch.read(bbuf, lower)
                        bbuf.flip()

                        return bbuf.slice(diff.toInt(), pathSlice.length.toInt()) to arena
                    } catch (e: ClosedByInterruptException) {
                        throw InterruptedException(e.message)
                    } catch (e : Exception) {
                        arena.close()
                        throw e
                    }

                }
            }
        }
    }

    private inner class Entry(
        val inner: IEntry<PathSlice>,
        val onEvict: AutoCloseable?,
        val bbuf: ByteBuffer,
        val arena: Arena
    ) : IEntry<PathSlice> by inner {
        override fun onEvict(k: PathSlice, reason: RemovalCause) {
            arena.close()
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
                var bbuf: ByteBuffer
                var arena: Arena
                if (pathSlice.offset == null || pathSlice.length == null) {
                    val (bb, a) = pathLoader.load(pathSlice.path)
                    bbuf = bb
                    arena = a
                } else {

                    val (bb, a) = pathLoader.load(pathSlice)
                    bbuf = bb
                    arena = a
                }
                Entry(pinningCache.Entry(bbuf.capacity().toLong()), onEvict, bbuf, arena)
            }
        }.get()!!

        val bbuf = entry.bbuf

        // create a new ArrowBuf for each request.
        // when the ref-count drops to zero, we release a ref-count in the cache.
        return allocator.wrapForeignAllocation(
            object : ForeignAllocation(bbuf.capacity().toLong(), MemoryUtil.getByteBufferAddress(bbuf)) {
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