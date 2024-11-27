@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import com.github.benmanes.caffeine.cache.RemovalCause
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import org.apache.arrow.memory.util.MemoryUtil
import xtdb.cache.PinningCache.IEntry
import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.CompletableFuture


data class PathSlice(val path: Path, val offset: Long? = null, val length: Long? = null) {
    constructor(path: Path) : this(path, null, null)
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

    val stats get() = pinningCache.stats

    interface PathLoader {
        fun load(path: Path): ByteBuffer
        fun load(pathSlice: PathSlice): ByteBuffer
        fun tryFree(bbuf: ByteBuffer) {}

        companion object {
            operator fun invoke() = object : PathLoader {
                override fun load(path: Path) =
                    try {
                        val ch = FileChannel.open(path)
                        val size = ch.size()

                        ch.map(FileChannel.MapMode.READ_ONLY, 0, size)
                    } catch (e: ClosedByInterruptException) {
                        throw InterruptedException(e.message)
                    }

                override fun load(pathSlice: PathSlice) =
                    try {
                        val ch = FileChannel.open(pathSlice.path)

                        ch.map(FileChannel.MapMode.READ_ONLY, pathSlice.offset!!, pathSlice.length!!)
                    } catch (e: ClosedByInterruptException) {
                        throw InterruptedException(e.message)
                    }

                override fun tryFree(bbuf: ByteBuffer) {
                    PlatformDependent.freeDirectBuffer(bbuf)
                }
            }
        }
    }

    private inner class Entry(
        val inner: IEntry<PathSlice>,
        val onEvict: AutoCloseable?,
        val bbuf: ByteBuffer
    ) : IEntry<PathSlice> by inner {
        override fun onEvict(k: PathSlice, reason: RemovalCause) {
            pathLoader.tryFree(bbuf)
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
                if (pathSlice.offset == null || pathSlice.length == null) {
                    bbuf = pathLoader.load(pathSlice.path)
                } else {
                    bbuf = pathLoader.load(pathSlice)
                }
                Entry(pinningCache.Entry(bbuf.capacity().toLong()), onEvict, bbuf)
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