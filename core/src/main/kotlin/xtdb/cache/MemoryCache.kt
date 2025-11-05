@file:Suppress("SpellCheckingInspection")

package xtdb.cache

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.Serializable
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.ForeignAllocation
import xtdb.util.closeOnCatch
import xtdb.util.maxDirectMemory
import xtdb.util.openReadableChannel
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import kotlin.io.path.fileSize

/**
 * NOTE: the allocation count metrics in the provided `allocator` WILL NOT be accurate
 *   - we allocate a buffer in here for every _usage_, and don't take shared memory into account.
 *
 * This isn't *caching* much now (see #4831) - it's more to keep track of/limit how much we've mmap'd in.
 */
class MemoryCache @JvmOverloads internal constructor(
    al: BufferAllocator,
    maxSizeBytes: Long,
    private val pathLoader: PathLoader = PathLoader()
) : AutoCloseable {
    private val al = al.newChildAllocator("memory-cache", 0, maxSizeBytes)

    data class Slice(val offset: Long, val length: Long) {
        companion object {
            fun from(path: Path) = Slice(0, path.fileSize())
        }
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

    @FunctionalInterface
    fun interface Fetch {
        /**
         * @return a pair containing the on-disk path and an optional cleanup action
         */
        suspend operator fun invoke(k: Path): Pair<Path, AutoCloseable?>
    }

    suspend fun get(key: Path, slice: Slice? = null, fetch: Fetch): ArrowBuf {
        val (path, onEvict) = fetch(key)
        val slice = slice ?: Slice.from(path)

        return try {
            // we open up a fine-grained arena here so that we can release the memory
            // as soon as we're done with the ArrowBuf.
            Arena.ofShared().closeOnCatch { arena ->
                val memSeg = pathLoader.load(path, slice, arena)

                al.wrapForeignAllocation(
                    object : ForeignAllocation(memSeg.byteSize(), memSeg.address()) {
                        override fun release0() {
                            arena.close()
                            onEvict?.close()
                        }
                    })
            }
        } catch (t: Throwable) {
            onEvict?.close()
            throw t
        }
    }

    override fun close() = al.close()

    companion object {
        @JvmStatic
        fun factory() = Factory()

        internal fun MeterRegistry.registerMemoryCache(cache: MemoryCache) {
            fun registerGauge(name: String, f: MemoryCache.() -> Long) {
                Gauge.builder(name, cache) { cache.f().toDouble() }
                    .baseUnit("bytes").register(this@registerMemoryCache)
            }

            registerGauge("memory-cache.pinnedBytes") { cache.al.allocatedMemory }
            registerGauge("memory-cache.evictableBytes") { 0 }
            registerGauge("memory-cache.freeBytes") { cache.al.let { it.limit - it.allocatedMemory } }
        }
    }
}
