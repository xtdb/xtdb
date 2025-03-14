@file:UseSerializers(PathWithEnvVarSerde::class)

package xtdb.api.storage

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import xtdb.BufferPool
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.Xtdb
import xtdb.buffer_pool.LocalBufferPool
import xtdb.buffer_pool.MemoryBufferPool
import xtdb.buffer_pool.RemoteBufferPool
import xtdb.util.StringUtil.asLexHex
import xtdb.util.closeOnCatch
import java.nio.file.Path

typealias StorageVersion = Int

object Storage {

    // bump this if the storage format changes in a backwards-incompatible way
    const val VERSION: StorageVersion = 6

    @JvmStatic
    fun storageRoot(version: StorageVersion): Path = Path.of("v${version.asLexHex}")

    @JvmField
    val STORAGE_ROOT: Path = storageRoot(VERSION)

    /**
     * Represents a factory interface for creating storage instances.
     * The default implementation is [InMemoryStorageFactory] which stores data in memory
     */
    @Serializable
    sealed interface Factory {
        fun open(
            allocator: BufferAllocator,
            meterRegistry: MeterRegistry = SimpleMeterRegistry(),
            storageVersion: StorageVersion = VERSION
        ): BufferPool
    }

    internal fun arrowFooterCache(maxEntries: Long = 1024): Cache<Path, ArrowFooter> =
        Caffeine.newBuilder().maximumSize(maxEntries).build()

    /**
     * Default implementation for the storage module when configuring an XTDB node.
     * Stores everything within in-process memory - a **non-persistent** option for storage.
     */
    @Serializable
    @SerialName("!InMemory")
    data object InMemoryStorageFactory : Factory {
        override fun open(allocator: BufferAllocator, meterRegistry: MeterRegistry, storageVersion: StorageVersion) =
            MemoryBufferPool(allocator, meterRegistry)
    }

    @JvmStatic
    fun inMemoryStorage() = InMemoryStorageFactory

    /**
     * Implementation for the storage module that persists data to the local file system, under the **path** directory.
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    localStorage(path = Paths.get("test-path")) {
     *        maxCacheEntries = 1024,
     *        maxCacheBytes = 536870912
     *    },
     *    ...
     * }
     * ```
     *
     * @property path The directory path where data will be stored.
     */
    @Serializable
    @SerialName("!Local")
    data class LocalStorageFactory(
        val path: Path,
        var maxCacheEntries: Long = 1024,
        var maxCacheBytes: Long? = null,
    ) : Factory {

        fun maxCacheEntries(maxCacheEntries: Long) = apply { this.maxCacheEntries = maxCacheEntries }
        fun maxCacheBytes(maxCacheBytes: Long) = apply { this.maxCacheBytes = maxCacheBytes }

        override fun open(allocator: BufferAllocator, meterRegistry: MeterRegistry, storageVersion: StorageVersion) =
            LocalBufferPool(this, storageVersion, allocator, meterRegistry)
    }

    @JvmStatic
    fun localStorage(path: Path) = LocalStorageFactory(path)

    @JvmSynthetic
    fun Xtdb.Config.localStorage(path: Path, configure: LocalStorageFactory.() -> Unit) =
        storage(LocalStorageFactory(path).also(configure))

    /**
     * Implementation for the storage module that persists data remotely within a specified [objectStore],
     * while maintaining a local cache of the working set cache under the [localDiskCache] directory.
     *
     * Any implementer of [ObjectStore.Factory] can be used as the [objectStore]. We currently offer:
     * * AWS S3 (under **xtdb-aws**)
     * * Azure Blob Storage (under **xtdb-azure**)
     * * Google Cloud Storage (under **xtdb-google-cloud**)
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    remoteStorage(
     *       objectStore = objStoreImpl(...) { ... },
     *       localDiskCache = Paths.get("test-path")
     *    ) {
     *       maxCacheEntries = 1024,
     *       maxCacheBytes = 536870912,
     *       maxDiskCachePercentage = 75,
     *       maxDiskCacheBytes = 10737418240
     *    },
     *    ...
     * }
     * ```
     *
     * @property objectStore configuration of the object store to use for remote storage.
     * @property localDiskCache local directory to store the working-set cache in.
     */
    @Serializable
    @SerialName("!Remote")
    data class RemoteStorageFactory(
        val objectStore: ObjectStore.Factory,
        val localDiskCache: Path,
        var maxCacheEntries: Long = 1024,
        var maxCacheBytes: Long? = null,
        var maxDiskCachePercentage: Long = 75,
        var maxDiskCacheBytes: Long? = null
    ) : Factory {

        fun maxCacheEntries(maxCacheEntries: Long) = apply { this.maxCacheEntries = maxCacheEntries }
        fun maxCacheBytes(maxCacheBytes: Long) = apply { this.maxCacheBytes = maxCacheBytes }
        fun maxDiskCachePercentage(maxDiskCachePercentage: Long) =
            apply { this.maxDiskCachePercentage = maxDiskCachePercentage }

        fun maxDiskCacheBytes(maxDiskCacheBytes: Long) = apply { this.maxDiskCacheBytes = maxDiskCacheBytes }

        override fun open(allocator: BufferAllocator, meterRegistry: MeterRegistry, storageVersion: StorageVersion) =
            objectStore.openObjectStore(storageRoot(storageVersion)).closeOnCatch { objectStore ->
                RemoteBufferPool(this, allocator, objectStore, meterRegistry)
            }
    }

    @JvmStatic
    fun remoteStorage(objectStore: ObjectStore.Factory, localDiskCachePath: Path) =
        RemoteStorageFactory(objectStore, localDiskCache = localDiskCachePath)

    @JvmSynthetic
    fun Xtdb.Config.remoteStorage(
        objectStore: ObjectStore.Factory,
        localDiskCachePath: Path,
        configure: RemoteStorageFactory.() -> Unit,
    ) = storage(RemoteStorageFactory(objectStore, localDiskCache = localDiskCachePath).also(configure))
}

