package xtdb.util

import org.apache.arrow.vector.ipc.SeekableReadChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

internal fun Path.newSeekableByteChannel() = SeekableReadChannel(Files.newByteChannel(this, READ))

val String.asPath: Path
    get() = Path.of(this)

fun Iterable<AutoCloseable>.closeAll() {
    forEach { it.close() }
}

inline fun <T : AutoCloseable, R> T.closeOnCatch(block: (T) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        close()
        throw e
    }

inline fun <C : AutoCloseable, L : Iterable<C>, R> L.closeAllOnCatch(block: (L) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        closeAll()
        throw e
    }

inline fun <C : AutoCloseable, L : Iterable<C>, R> L.useAll(block: (L) -> R): R =
    try {
        block(this)
    } finally {
        closeAll()
    }