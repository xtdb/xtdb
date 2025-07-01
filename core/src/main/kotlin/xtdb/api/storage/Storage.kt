@file:UseSerializers(PathWithEnvVarSerde::class)

package xtdb.api.storage

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import xtdb.BufferPool
import xtdb.api.PathWithEnvVarSerde
import xtdb.buffer_pool.LocalBufferPool
import xtdb.buffer_pool.MemoryBufferPool
import xtdb.buffer_pool.RemoteBufferPool
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.util.StringUtil.asLexHex
import xtdb.util.closeOnCatch
import java.nio.file.Path
import kotlin.io.path.createDirectories

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
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            meterRegistry: MeterRegistry? = null,
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
        override fun open(
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            meterRegistry: MeterRegistry?, storageVersion: StorageVersion
        ): BufferPool = MemoryBufferPool(allocator, meterRegistry)
    }

    @JvmStatic
    fun inMemoryStorage() = InMemoryStorageFactory

    /**
     * Implementation for the storage module that persists data to the local file system, under the **path** directory.
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    localStorage(path = Paths.get("test-path")),
     *    ...
     * }
     * ```
     *
     * @property path The directory path where data will be stored.
     */
    @Serializable
    @SerialName("!Local")
    data class LocalStorageFactory(val path: Path) : Factory {

        override fun open(
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            meterRegistry: MeterRegistry?, storageVersion: StorageVersion
        ): BufferPool {
            val diskStore = path.resolve(storageRoot(storageVersion)).also { it.createDirectories() }

            return LocalBufferPool(allocator, memoryCache, meterRegistry, diskStore)
        }
    }

    @JvmStatic
    fun localStorage(path: Path) = LocalStorageFactory(path)

    /**
     * Implementation for the storage module that persists data remotely within a specified [objectStore],
     *
     * Any implementer of [ObjectStore.Factory] can be used as the [objectStore]. We currently offer:
     * * AWS S3 (under **xtdb-aws**)
     * * Azure Blob Storage (under **xtdb-azure**)
     * * Google Cloud Storage (under **xtdb-google-cloud**)
     *
     * @property objectStore configuration of the object store to use for remote storage.
     */
    @Serializable
    @SerialName("!Remote")
    data class RemoteStorageFactory(val objectStore: ObjectStore.Factory) : Factory {

        override fun open(
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            meterRegistry: MeterRegistry?, storageVersion: StorageVersion
        ): BufferPool {
            requireNotNull(diskCache) { "diskCache is required for remote storage" }

            return objectStore.openObjectStore(storageRoot(storageVersion)).closeOnCatch { objectStore ->
                RemoteBufferPool(allocator, objectStore, memoryCache, diskCache, meterRegistry)
            }
        }
    }

    @JvmStatic
    fun remoteStorage(objectStore: ObjectStore.Factory) =
        RemoteStorageFactory(objectStore)
}

