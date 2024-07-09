@file:UseSerializers(PathWithEnvVarSerde::class)

package xtdb.api.storage

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.arrow.memory.BufferAllocator
import xtdb.IBufferPool
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.Xtdb
import xtdb.util.requiringResolve
import java.nio.file.Path

object Storage {

    /**
     * Represents a factory interface for creating storage instances.
     * The default implementation is [InMemoryStorageFactory] which stores data in memory
     */
    @Serializable
    sealed interface Factory {
        fun openStorage(allocator: BufferAllocator): IBufferPool
    }

    /**
     * Default implementation for the storage module when configuring an XTDB node. Stores everything within in-process memory -
     * a **non-persistent** option for storage.
     */
    @Serializable
    @SerialName("!InMemory")
    data object InMemoryStorageFactory : Factory {

        override fun openStorage(allocator: BufferAllocator) =
            requiringResolve("xtdb.buffer-pool/open-in-memory-storage").invoke(allocator) as IBufferPool
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
        var maxCacheBytes: Long = 536870912,
    ) : Factory {

        fun maxCacheEntries(maxCacheEntries: Long) = apply { this.maxCacheEntries = maxCacheEntries }
        fun maxCacheBytes(maxCacheBytes: Long) = apply { this.maxCacheBytes = maxCacheBytes }

        override fun openStorage(allocator: BufferAllocator) =
            requiringResolve("xtdb.buffer-pool/open-local-storage").invoke(allocator, this) as IBufferPool
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
     * Any implementer of [ObjectStoreFactory] can be used as the [objectStore]. We currently offer:
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
     *       maxCacheBytes = 536870912
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
        val objectStore: ObjectStoreFactory,
        val localDiskCache: Path,
        var maxCacheEntries: Long = 1024,
        var maxCacheBytes: Long = 536870912,
    ) : Factory {

        fun maxCacheEntries(maxCacheEntries: Long) = apply { this.maxCacheEntries = maxCacheEntries }
        fun maxCacheBytes(maxCacheBytes: Long) = apply { this.maxCacheBytes = maxCacheBytes }

        override fun openStorage(allocator: BufferAllocator) =
            requiringResolve("xtdb.buffer-pool/open-remote-storage").invoke(allocator, this) as IBufferPool
    }

    @JvmStatic
    fun remoteStorage(objectStore: ObjectStoreFactory, localDiskCachePath: Path) =
        RemoteStorageFactory(objectStore, localDiskCachePath)

    @JvmSynthetic
    fun Xtdb.Config.remoteStorage(
        objectStore: ObjectStoreFactory,
        localDiskCachePath: Path,
        configure: RemoteStorageFactory.() -> Unit,
    ) = storage(RemoteStorageFactory(objectStore, localDiskCachePath).also(configure))
}

