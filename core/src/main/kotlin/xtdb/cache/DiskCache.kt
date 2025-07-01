@file:UseSerializers(PathWithEnvVarSerde::class)
package xtdb.cache

import com.github.benmanes.caffeine.cache.RemovalCause
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.slf4j.LoggerFactory
import xtdb.api.PathWithEnvVarSerde
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

private val LOGGER = LoggerFactory.getLogger(DiskCache::class.java)

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
            LOGGER.trace("Evicted {} due to {}", k, reason)
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

        LOGGER.debug("disk cache started, existing size: {} bytes", pinningCache.stats0.evictableBytes)
    }

    @Suppress("MemberVisibilityCanBePrivate")
    fun createTempPath(): Path =
        Files.createTempFile(rootPath.resolve(".tmp").createDirectories(), "upload", ".arrow")

    @FunctionalInterface
    fun interface Fetch {
        operator fun invoke(k: Path, tmpFile: Path): CompletableFuture<Path>
    }

    @Suppress("NAME_SHADOWING")
    fun get(k: Path, fetch: Fetch) =
        pinningCache.get(k) { k ->
            val diskCachePath = rootPath.resolve(k)

            if (diskCachePath.exists())
                completedFuture(Entry(k, diskCachePath))
            else
                fetch(k, createTempPath())
                    .thenApply { tmpPath ->
                        tmpPath.moveTo(diskCachePath.createParentDirectories(), ATOMIC_MOVE, REPLACE_EXISTING)
                        Entry(k, diskCachePath)
                    }
        }

    @Suppress("NAME_SHADOWING")
    fun put(k: Path, tmpFile: Path) {
        pinningCache.cache.asMap()
            .computeIfAbsent(k) { k ->
                val diskCachePath = rootPath.resolve(k)
                tmpFile.moveTo(diskCachePath.createParentDirectories(), ATOMIC_MOVE, REPLACE_EXISTING)
                completedFuture(Entry(pinningCache.Entry(diskCachePath.fileSize()), k, diskCachePath))
            }
    }

    companion object {
        @JvmStatic
        fun factory(path: Path) = Factory(path)

        fun MeterRegistry.registerDiskCache(cache: DiskCache) {
            cache.pinningCache.registerMetrics("disk-cache", this)

            Gauge.builder("disk-cache.maxSizeBytes", cache) { it.maxSizeBytes.toDouble() }
                .baseUnit("bytes").register(this)
        }

    }
}
