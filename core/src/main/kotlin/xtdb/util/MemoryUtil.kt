@file:JvmName("MemoryUtil")
package xtdb.util

import io.netty.util.internal.PlatformDependent
import java.lang.Runtime.getRuntime
import java.nio.MappedByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

fun <T : AutoCloseable, R> T.closeOnCatch(block: (T) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        close()
        throw e
    }

val maxDirectMemory =
    try {
        PlatformDependent.maxDirectMemory()
    } catch (e: Throwable) {
        // otherwise we use as much direct memory as there was heap specified
        getRuntime().maxMemory()
    }

val usedNettyMemory get() = PlatformDependent.usedDirectMemory()

fun toMmapPath(path: Path): MappedByteBuffer =
    try {
        FileChannel.open(path, StandardOpenOption.READ).use { channel ->
            channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
        }
    } catch (e: ClosedByInterruptException) {
        throw InterruptedException()
    }