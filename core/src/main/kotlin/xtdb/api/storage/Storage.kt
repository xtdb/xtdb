@file:UseSerializers(PathSerde::class)

package xtdb.api.storage

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowFooter
import xtdb.api.PathSerde
import xtdb.api.Remote
import xtdb.api.RemoteAlias
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

    internal fun validatePartition(partition: Int, totalPartitions: Int) {
        require(totalPartitions >= 1) { "totalPartitions must be >= 1, got $totalPartitions" }
        require(partition in 0 until totalPartitions) {
            "partition must be in 0..<$totalPartitions, got $partition"
        }
    }

    /**
     * At `totalPartitions == 1` this is byte-identical to the single-partition layout, so existing
     * object stores and local directories keep working without a key-space migration — load-bearing
     * for rolling upgrades, since blob stores have no `mv` primitive and re-keying a database would
     * mean a full copy-and-delete sweep. Only at N > 1 does the `parts/<partition>/` grouping appear.
     * Partition counts are immutable post-attach, so a store is one shape or the other for its whole
     * lifetime and the two never collide.
     */
    @JvmStatic
    fun storageRoot(version: StorageVersion, epoch: Int, partition: Int, totalPartitions: Int): Path {
        validatePartition(partition, totalPartitions)
        val root = storageRoot(version, epoch)
        return if (totalPartitions == 1) root else Path.of("parts", partition.toString()).resolve(root)
    }

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
            partition: Int = 0, totalPartitions: Int = 1,
            meterRegistry: MeterRegistry? = null,
            storageVersion: StorageVersion = VERSION,
            remotes: Map<RemoteAlias, Remote> = emptyMap(),
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
            dbName: DatabaseName, partition: Int, totalPartitions: Int,
            meterRegistry: MeterRegistry?, storageVersion: StorageVersion,
            remotes: Map<RemoteAlias, Remote>,
        ): BufferPool {
            // each open() is its own store, so partitions are isolated by construction — only the
            // index needs validating
            validatePartition(partition, totalPartitions)
            return MemoryStorage(allocator, epoch)
        }
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
            dbName: DatabaseName, partition: Int, totalPartitions: Int,
            meterRegistry: MeterRegistry?, storageVersion: StorageVersion,
            remotes: Map<RemoteAlias, Remote>,
        ): BufferPool {
            val rootPath = path.resolve(storageRoot(storageVersion, epoch, partition, totalPartitions))
                .also { it.createDirectories() }

            return LocalStorage(allocator, memoryCache, meterRegistry, epoch, dbName, partition, rootPath)
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
            dbName: DatabaseName, partition: Int, totalPartitions: Int,
            meterRegistry: MeterRegistry?, storageVersion: StorageVersion,
            remotes: Map<RemoteAlias, Remote>,
        ): BufferPool {
            requireNotNull(diskCache) { "diskCache is required for remote storage" }

            val objStoreRoot = storageRoot(storageVersion, epoch, partition, totalPartitions)
            return objectStore.openObjectStore(objStoreRoot, remotes).closeOnCatch { objectStore ->
                RemoteBufferPool(allocator, objectStore, memoryCache, diskCache, meterRegistry, epoch, dbName, partition)
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

