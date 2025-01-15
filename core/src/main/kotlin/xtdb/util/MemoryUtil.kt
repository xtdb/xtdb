package xtdb.util

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

fun toMmapPath(path : Path): MappedByteBuffer =
    try {
        FileChannel.open(path, StandardOpenOption.READ).use { channel ->
            channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
        }
    } catch (e: ClosedByInterruptException) {
        throw InterruptedException()
    }