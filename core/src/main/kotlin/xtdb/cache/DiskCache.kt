@file:UseSerializers(PathSerde::class)
package xtdb.cache

import com.github.benmanes.caffeine.cache.RemovalCause
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import xtdb.api.PathSerde
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.attribute.BasicFileAttributes
import java.util.Comparator.comparingLong
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.*
import kotlin.math.max
import xtdb.util.logger
import xtdb.util.trace
import xtdb.util.debug

private val LOGGER = DiskCache::class.logger

class DiskCache internal constructor(val rootPath: Path, val maxSizeBytes: Long) {

    @Serializable
    data class Factory
    @JvmOverloads constructor(
        val path: Path,
        var maxSizeBytes: Long? = null,
        var maxSizeRatio: Double = 0.75,
    ) {
        fun maxSizeBytes(maxSizeBytes: Long?) = apply { this.maxSizeBytes = maxSizeBytes }
        fun maxSizeRatio(maxSizeRatio: Double) = apply { this.maxSizeRatio = maxSizeRatio }

        fun build(meterRegistry: MeterRegistry? = null) =
            DiskCache(
                path.also { it.createDirectories() },
                maxSizeBytes ?: (Files.getFileStore(path).totalSpace * maxSizeRatio).toLong()
            ).also { meterRegistry?.registerDiskCache(it) }
    }

    val pinningCache = PinningCache<Path, Entry>(maxSizeBytes)

    val stats get() = pinningCache.stats

    inner class Entry(
        inner: PinningCache.IEntry<Path>,
        val k: Path,
        val path: Path,
    ) : PinningCache.IEntry<Path> by inner, AutoCloseable {

        override fun onEvict(k: Path, reason: RemovalCause) {
            path.deleteIfExists()
            LOGGER.trace("Evicted $k due to $reason")
            super.onEvict(k, reason)
        }

        constructor(k: Path, path: Path) : this(pinningCache.Entry(path.fileSize()), k, path)

        override fun close() {
            pinningCache.releaseEntry(k)
        }
    }

    init {
        LOGGER.debug("Creating disk cache with maxSizeBytes=$maxSizeBytes")
        val syncInnerCache = pinningCache.cache.synchronous()

        rootPath.resolve(TMP_DIR).toFile().deleteRecursively()

        Files.walk(rootPath)
            .filter { Files.isRegularFile(it) }
            .sorted(comparingLong { path ->
                Files.readAttributes(path, BasicFileAttributes::class.java).let { attrs ->
                    max(attrs.lastAccessTime().toMillis(), attrs.lastModifiedTime().toMillis())
                }
            })
            .forEach { path ->
                val k = rootPath.relativize(path)
                syncInnerCache.put(k, Entry(k, path))
            }

        LOGGER.debug("disk cache started, existing size: ${pinningCache.stats0.evictableBytes} bytes")
    }

    internal fun createTempPath(): Path =
        Files.createTempFile(rootPath.resolve(TMP_DIR).createDirectories(), "upload", ".arrow")

    @FunctionalInterface
    fun interface Fetch {
        operator fun invoke(k: Path, tmpFile: Path): CompletableFuture<Path>
    }

    /**
     * A view of this cache whose keys are namespaced under [prefix].
     *
     * The cache is shared by every buffer pool on the node; each pool reads and writes through its own scope,
     * passing keys relative to its store — [get]'s [Fetch] is handed the same relative key.
     */
    inner class Scope internal constructor(private val prefix: Path) {
        fun get(k: Path, fetch: Fetch): CompletableFuture<Entry> =
            this@DiskCache.get(prefix.resolve(k)) { cacheKey, tmpFile -> fetch(prefix.relativize(cacheKey), tmpFile) }

        /** Adopts [tmpFile] as the entry for [k], unless an entry for [k] already exists. */
        fun put(k: Path, tmpFile: Path) = this@DiskCache.put(prefix.resolve(k), tmpFile)

        fun createTempPath(): Path = this@DiskCache.createTempPath()
    }

    fun scope(prefix: Path) = Scope(prefix)

    @Suppress("NAME_SHADOWING")
    internal fun get(k: Path, fetch: Fetch) =
        pinningCache.get(k) { k ->
            val diskCachePath = rootPath.resolve(k)

            if (diskCachePath.exists())
                completedFuture(Entry(k, diskCachePath))
            else {
                val tmpPath = createTempPath()
                fetch(k, tmpPath)
                    .thenApply {
                        tmpPath.moveTo(diskCachePath.createParentDirectories(), ATOMIC_MOVE, REPLACE_EXISTING)
                        Entry(k, diskCachePath)
                    }
                    .whenComplete { _, ex ->
                        if (ex != null) tmpPath.deleteIfExists()
                    }
            }
        }

    @Suppress("NAME_SHADOWING")
    internal fun put(k: Path, tmpFile: Path) {
        pinningCache.cache.asMap()
            .computeIfAbsent(k) { k ->
                val diskCachePath = rootPath.resolve(k)
                tmpFile.moveTo(diskCachePath.createParentDirectories(), ATOMIC_MOVE, REPLACE_EXISTING)
                completedFuture(Entry(pinningCache.Entry(diskCachePath.fileSize()), k, diskCachePath))
            }
    }

    companion object {
        private const val TMP_DIR = ".tmp"

        @JvmStatic
        fun factory(path: Path) = Factory(path)

        fun MeterRegistry.registerDiskCache(cache: DiskCache) {
            cache.pinningCache.registerMetrics("disk-cache", this)

            Gauge.builder("disk-cache.maxSizeBytes", cache) { it.maxSizeBytes.toDouble() }
                .baseUnit("bytes").register(this)
        }

    }
}
