package xtdb.util

import java.nio.channels.SeekableByteChannel
import java.nio.channels.WritableByteChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists

internal fun Path.openReadableChannel(): SeekableByteChannel = Files.newByteChannel(this, READ)
internal fun Path.openWritableChannel(): WritableByteChannel = Files.newByteChannel(this, WRITE, CREATE)

internal fun <R> useTempFile(prefix: String, suffix: String, block: (Path) -> R): R =
    createTempFile(prefix, suffix).let {
        try {
            block(it)
        } finally {
            it.deleteIfExists()
        }
    }

val String.asPath: Path
    get() = Path.of(this)

fun Iterable<AutoCloseable?>.closeAll() {
    forEach { it?.close() }
}

fun <K, V : AutoCloseable?> Map<K, V>.closeAll() {
    values.closeAll()
}

inline fun <T : AutoCloseable?, R> T.closeOnCatch(block: (T) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        this?.close()
        throw e
    }

inline fun <C : AutoCloseable?, L : Iterable<C>, R> L.closeAllOnCatch(block: (L) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        closeAll()
        throw e
    }

inline fun <C : AutoCloseable?, L : Iterable<C>, R> L.useAll(block: (L) -> R): R =
    try {
        block(this)
    } finally {
        closeAll()
    }

inline fun <C, L : Iterable<C>, R : AutoCloseable?> L.safeMap(block: (C) -> R): List<R> =
    mutableListOf<R>().closeAllOnCatch { els ->
        for (el in this) {
            els.add(block(el))
        }

        els
    }