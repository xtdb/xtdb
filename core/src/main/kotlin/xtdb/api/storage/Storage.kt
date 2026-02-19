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
import xtdb.api.PathWithEnvVarSerde
import xtdb.cache.DiskCache
import xtdb.cache.MemoryCache
import xtdb.database.DatabaseName
import xtdb.database.proto.DatabaseConfig
import xtdb.database.proto.DatabaseConfig.StorageCase.*
import xtdb.database.proto.RemoteStorage
import xtdb.database.proto.inMemoryStorage
import xtdb.database.proto.localStorage
import xtdb.storage.BufferPool
import xtdb.storage.LocalStorage
import xtdb.storage.MemoryStorage
import xtdb.storage.RemoteBufferPool
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import xtdb.util.closeOnCatch
import java.nio.file.Path
import kotlin.io.path.createDirectories

typealias StorageVersion = Int

object Storage {

    // bump this if the storage format changes in a backwards-incompatible way
    const val VERSION: StorageVersion = 6

    @JvmStatic
    fun storageRoot(version: StorageVersion, epoch: Int): Path =
        Path.of(buildString {
            append("v${version.asLexHex}")
            if (epoch > 0) append("_e${epoch.asLexHex}")
        })

    /**
     * Represents a factory interface for creating storage instances.
     * The default implementation is [InMemoryStorageFactory] which stores data in memory
     */
    @Serializable
    sealed interface Factory {
        var epoch: Int

        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        fun open(
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            dbName: DatabaseName,
            meterRegistry: MeterRegistry? = null,
            storageVersion: StorageVersion = VERSION
        ): BufferPool

        companion object {
            internal fun fromProto(config: DatabaseConfig): Factory =
                when (config.storageCase) {
                    IN_MEMORY_STORAGE -> InMemoryStorageFactory(config.inMemoryStorage.epoch)
                    LOCAL_STORAGE -> config.localStorage.let { LocalStorageFactory(it.path.asPath, it.epoch) }

                    REMOTE_STORAGE ->
                        config.remoteStorage.let {
                            RemoteStorageFactory(ObjectStore.Factory.fromProto(it.objectStore), it.epoch)
                        }

                    else -> error("invalid storage: ${config.storageCase}")
                }
        }
    }

    internal fun arrowFooterCache(maxEntries: Long = 1024): Cache<Path, ArrowFooter> =
        Caffeine.newBuilder().maximumSize(maxEntries).build()

    /**
     * Default implementation for the storage module when configuring an XTDB node.
     * Stores everything within in-process memory - a **non-persistent** option for storage.
     */
    @Serializable
    @SerialName("!InMemory")
    data class InMemoryStorageFactory(override var epoch: Int = 0) : Factory {
        override fun open(
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            dbName: DatabaseName, meterRegistry: MeterRegistry?, storageVersion: StorageVersion
        ): BufferPool = MemoryStorage(allocator, epoch)
    }

    @JvmStatic
    fun inMemory() = InMemoryStorageFactory()

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
    data class LocalStorageFactory(val path: Path, override var epoch: Int = 0) : Factory {

        override fun open(
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            dbName: DatabaseName, meterRegistry: MeterRegistry?, storageVersion: StorageVersion
        ): BufferPool {
            val rootPath = path.resolve(storageRoot(storageVersion, epoch)).also { it.createDirectories() }

            return LocalStorage(allocator, memoryCache, meterRegistry, epoch, dbName, rootPath)
        }
    }

    @JvmStatic
    fun local(path: Path) = LocalStorageFactory(path)

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
    data class RemoteStorageFactory(val objectStore: ObjectStore.Factory, override var epoch: Int = 0) : Factory {

        override fun open(
            allocator: BufferAllocator, memoryCache: MemoryCache, diskCache: DiskCache?,
            dbName: DatabaseName, meterRegistry: MeterRegistry?, storageVersion: StorageVersion
        ): BufferPool {
            requireNotNull(diskCache) { "diskCache is required for remote storage" }

            return objectStore.openObjectStore(storageRoot(storageVersion, epoch)).closeOnCatch { objectStore ->
                RemoteBufferPool(allocator, objectStore, memoryCache, diskCache, meterRegistry, epoch, dbName)
            }
        }
    }

    @JvmStatic
    fun remote(objectStore: ObjectStore.Factory) =
        RemoteStorageFactory(objectStore)

    fun DatabaseConfig.Builder.applyStorage(storage: Storage.Factory) {
        when (storage) {
            is InMemoryStorageFactory -> setInMemoryStorage(inMemoryStorage { epoch = storage.epoch })
            is LocalStorageFactory -> setLocalStorage(localStorage {
                this.path = storage.path.toString()
                epoch = storage.epoch
            })

            is RemoteStorageFactory ->
                setRemoteStorage(
                    RemoteStorage.newBuilder()
                        .setObjectStore(storage.objectStore.configProto)
                        .setEpoch(storage.epoch)
                        .build()
                )

        }
    }
}

