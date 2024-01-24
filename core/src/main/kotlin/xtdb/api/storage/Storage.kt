@file:JvmName("Storage")
@file:UseSerializers(PathWithEnvVarSerde::class)

package xtdb.api.storage

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.arrow.memory.BufferAllocator
import xtdb.IBufferPool
import xtdb.api.PathWithEnvVarSerde
import xtdb.util.requiringResolve
import java.nio.file.Path

/**
 * Represents a factory interface for creating storage instances.
 * The default implementation is [InMemoryStorageFactory] which stores data in memory
 * */
@Serializable
sealed interface StorageFactory {
    companion object {
        val DEFAULT = InMemoryStorageFactory()
    }

    fun openStorage(allocator: BufferAllocator): IBufferPool
}

/**
 * Default implementation for the storage module when configuring an XTDB node. Stores everything within in-process memory -
 * a **non-persistent** option for storage.
 */
@Serializable
@SerialName("!InMemory")
class InMemoryStorageFactory() : StorageFactory {
    companion object {
        private val OPEN_STORAGE = requiringResolve("xtdb.buffer-pool", "open-in-memory-storage")
    }
    override fun openStorage(allocator: BufferAllocator) = OPEN_STORAGE.invoke(allocator) as IBufferPool
}

/**
 * Implementation for the storage module that persists data to the local file system, under the **path** directory.
 *
 * @property path The directory path where data will be stored.
 */
@Serializable
@SerialName("!Local")
data class LocalStorageFactory(
    val path: Path,
    var maxCacheEntries: Long = 1024,
    var maxCacheBytes: Long = 536870912,
) : StorageFactory {
    companion object {
        private val OPEN_STORAGE = requiringResolve("xtdb.buffer-pool", "open-local-storage")
    }

    fun maxCacheEntries(maxCacheEntries: Long) = apply { this.maxCacheEntries = maxCacheEntries }
    fun maxCacheBytes(maxCacheBytes: Long) = apply { this.maxCacheBytes = maxCacheBytes }

    override fun openStorage(allocator: BufferAllocator) = OPEN_STORAGE.invoke(allocator, this) as IBufferPool
}

fun local(path: Path) = LocalStorageFactory(path)

@JvmSynthetic
fun local(path: Path, build: LocalStorageFactory.() -> Unit) = LocalStorageFactory(path).also(build)

interface ObjectStoreFactory {
    fun openObjectStore(): ObjectStore
}

/**
 * Implementation for the storage module that persists data remotely within a specified **objectStore**, while maintaining a local
 * cache of the working set cache under the **localDiskCache** directory.
 *
 * Any implementer of [ObjectStoreFactory] can be used as the **objectStore**, the ones we currently offer being:
 * * [S3ObjectStoreFactory] (under **xtdb-s3**)
 * * [AzureObjectStoreFactory] (under **xtdb-azure**)
 * * [GoogleCloudObjectStoreFactory] (under **xtdb-google-cloud**)
 *
 * @property objectStore Configuration of the Object Store we want to use for remote storage.
 * @property localDiskCache Local directory that we store the working-set cache on.
 */
@Serializable
@SerialName("!Remote")
data class RemoteStorageFactory(
    val objectStore: ObjectStoreFactory,
    val localDiskCache: Path,
    var maxCacheEntries: Long = 1024,
    var maxCacheBytes: Long = 536870912,
) : StorageFactory {
    companion object {
        private val OPEN_STORAGE = requiringResolve("xtdb.buffer-pool", "open-remote-storage")
    }

    fun maxCacheEntries(maxCacheEntries: Long) = apply { this.maxCacheEntries = maxCacheEntries }
    fun maxCacheBytes(maxCacheBytes: Long) = apply { this.maxCacheBytes = maxCacheBytes }

    override fun openStorage(allocator: BufferAllocator) = OPEN_STORAGE.invoke(allocator, this) as IBufferPool
}

fun remote(objectStore: ObjectStoreFactory, localDiskCachePath: Path) =
    RemoteStorageFactory(objectStore, localDiskCachePath)

@JvmSynthetic
fun remote(objectStore: ObjectStoreFactory, localDiskCachePath: Path, build: RemoteStorageFactory.() -> Unit) =
    RemoteStorageFactory(objectStore, localDiskCachePath).also(build)

