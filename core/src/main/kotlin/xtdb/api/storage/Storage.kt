@file:JvmName("Storage")

package xtdb.api.storage

import org.apache.arrow.memory.BufferAllocator
import xtdb.IBufferPool
import xtdb.util.requiringResolve
import java.nio.file.Path

sealed interface StorageFactory {
    companion object {
        val DEFAULT = InMemoryStorageFactory
    }

    fun openStorage(allocator: BufferAllocator): IBufferPool
}

data object InMemoryStorageFactory : StorageFactory {
    private val OPEN_STORAGE = requiringResolve("xtdb.buffer-pool", "open-in-memory-storage")

    override fun openStorage(allocator: BufferAllocator) = OPEN_STORAGE.invoke(allocator) as IBufferPool
}

class LocalStorageFactory(
    val dataDirectory: Path,
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

fun local(dataDirectory: Path) = LocalStorageFactory(dataDirectory)

@JvmSynthetic
fun local(dataDirectory: Path, build: LocalStorageFactory.() -> Unit) = LocalStorageFactory(dataDirectory).also(build)

interface ObjectStoreFactory {
    fun openObjectStore(): ObjectStore
}

class RemoteStorageFactory(
    val objectStore: ObjectStoreFactory,
    val diskStore: Path,
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

fun remote(objectStore: ObjectStoreFactory, diskStore: Path) = RemoteStorageFactory(objectStore, diskStore)

@JvmSynthetic
fun remote(objectStore: ObjectStoreFactory, diskStore: Path, build: RemoteStorageFactory.() -> Unit) =
    RemoteStorageFactory(objectStore, diskStore).also(build)

