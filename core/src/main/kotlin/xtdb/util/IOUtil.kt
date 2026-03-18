package xtdb.util

import java.nio.channels.FileChannel
import java.nio.channels.WritableByteChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind.EXACTLY_ONCE
import kotlin.contracts.contract
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists

internal fun Path.openReadableChannel(): FileChannel = FileChannel.open(this, READ)
internal fun Path.openWritableChannel(): WritableByteChannel = Files.newByteChannel(this, WRITE, CREATE)

val String.asPath: Path
    get() = Path.of(this)

fun Iterable<AutoCloseable?>.closeAll() {
    forEach { it?.close() }
}

fun <K, V : AutoCloseable?> Map<K, V>.closeAll() {
    values.closeAll()
}

@OptIn(ExperimentalContracts::class)
inline fun <T : AutoCloseable?, R> T.closeOnCatch(block: (T) -> R): R {
    contract { callsInPlace(block, EXACTLY_ONCE) }

    return try {
        block(this)
    } catch (e: Throwable) {
        this?.close()
        throw e
    }
}

inline fun <C : AutoCloseable?, L : Iterable<C>, R> L.closeAllOnCatch(block: (L) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        closeAll()
        throw e
    }

inline fun <K, C : AutoCloseable?, M : Map<K, C>, R> M.closeAllOnCatch(block: (M) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        closeAll()
        throw e
    }

inline fun <R> Path.deleteOnCatch(block: (Path) -> R): R =
    try {
        block(this)
    } catch (e: Throwable) {
        deleteIfExists()
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

inline fun <C, L : Iterable<C>, R : AutoCloseable?> L.safeMapIndexed(block: (Int, C) -> R): List<R> =
    mutableListOf<R>().closeAllOnCatch { els ->
        for ((idx, el) in this.withIndex()) {
            els.add(block(idx, el))
        }

        els
    }

class SafelyOpeningScope {
    val openedResources = mutableListOf<AutoCloseable>()

    fun <C : AutoCloseable?> open(block: () -> C): C =
        block().also { if (it != null) openedResources.add(it) }

    fun <C : AutoCloseable> openAll(block: () -> Iterable<C>): Iterable<C> =
        block().also { openedResources.addAll(it) }

    fun <K, C : AutoCloseable> openAllMap(block: () -> Map<K, C>): Map<K, C> =
        block().also { openedResources.addAll(it.values) }

    fun openTempFile(prefix: String, suffix: String): Path =
        createTempFile(prefix, suffix).also { path ->
            openedResources.add(AutoCloseable { path.deleteIfExists() })
        }
}

inline fun <R> safelyOpening(block: SafelyOpeningScope.() -> R): R {
    val scope = SafelyOpeningScope()
    return try {
        scope.block()
    } catch (e: Throwable) {
        scope.openedResources.closeAll()
        throw e
    }
}
