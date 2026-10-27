@file:JvmName("MemoryUtil")
package xtdb.util

import io.netty.buffer.PooledByteBufAllocator
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.AllocationListener
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.memory.rounding.RoundingPolicy
import java.lang.Runtime.getRuntime
import java.nio.MappedByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

val maxDirectMemory =
    try {
        PlatformDependent.maxDirectMemory()
    } catch (e: Throwable) {
        // otherwise we use as much direct memory as there was heap specified
        getRuntime().maxMemory()
    }

val usedNettyMemory get() = PlatformDependent.usedDirectMemory()

private val nettyChunkSize = PooledByteBufAllocator.defaultPageSize().toLong() shl PooledByteBufAllocator.defaultMaxOrder()

/**
 * The size Netty's pooled allocator actually takes for a request: jemalloc's size classes, four per doubling (16-byte steps up to 128),
 * and anything larger than a chunk exactly - see Netty's `SizeClasses`.
 */
internal fun nettySizeClass(size: Long): Long = when {
    size <= 0 || size > nettyChunkSize -> size
    size <= 128 -> (size + 15) and 15L.inv()
    else -> {
        val delta = 1L shl (63 - (size - 1).countLeadingZeroBits() - 2)
        (size + delta - 1) and (delta - 1).inv()
    }
}

/**
 * A root allocator that accounts each buffer at the size Netty allocates for it.
 * Arrow's default rounds requests under 16 MiB up to a power of two, which would waste ~30% of the memory cache's pages.
 */
@JvmOverloads
fun openRootAllocator(limit: Long = Long.MAX_VALUE, listener: AllocationListener = AllocationListener.NOOP) =
    RootAllocator(listener, limit, RoundingPolicy(::nettySizeClass))

fun toMmapPath(path: Path): MappedByteBuffer =
    try {
        FileChannel.open(path, StandardOpenOption.READ).use { channel ->
            channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
        }
    } catch (e: ClosedByInterruptException) {
        throw InterruptedException()
    }