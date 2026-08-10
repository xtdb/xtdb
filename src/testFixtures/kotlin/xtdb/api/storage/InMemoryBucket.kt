package xtdb.api.storage

import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.multipart.IMultipartUpload
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.CompletableFuture.failedFuture

enum class StoreOperation {
    PUT, UPLOAD, COMPLETE, ABORT
}

/**
 * The durable-bucket half of the remote-storage test doubles: a keyed blob map plus an operation
 * journal. Deliberately NOT an [ObjectStore] — that is the client role, played by [PrefixedObjectStore]
 * over this. Keeping the bucket off the client interface makes "wire a pool straight to the bucket,
 * skipping the prefix" a compile error rather than a silent vacuous pass.
 */
class InMemoryBucket(
    // Synchronized: uploadMultipartBuffers calls uploadPart from concurrent coroutines (up to
    // MAX_CONCURRENT_PART_UPLOADS), so a plain ArrayList races on add and intermittently throws AIOOBE.
    val calls: MutableList<StoreOperation> = Collections.synchronizedList(mutableListOf()),
    // Concurrent: one bucket backs several pools that may write at once (e.g. per-partition pools),
    // so a plain TreeMap would corrupt under concurrent mutation — the same reason `calls` is synchronized.
    val buffers: NavigableMap<Path, ByteBuffer> = ConcurrentSkipListMap()
) {

    private fun copyByteBuffer(buffer: ByteBuffer) =
        ByteBuffer.allocate(buffer.remaining()).put(buffer.duplicate()).flip()

    private fun concatByteBuffers(parts: List<ByteBuffer>): ByteBuffer {
        val totalSize = parts.sumOf { it.remaining() }
        val buffer = ByteBuffer.allocate(totalSize)
        parts.forEach { buffer.put(it.duplicate()) }
        return buffer.flip()
    }

    suspend fun getObject(k: Path): ByteBuffer =
        buffers[k] ?: throw IllegalStateException("Object $k doesn't exist")

    suspend fun getObject(k: Path, outPath: Path): Path {
        val buffer = buffers[k] ?: throw IllegalStateException("Object $k doesn't exist")
        val bytes = ByteArray(buffer.remaining())
        buffer.duplicate().get(bytes)
        outPath.toFile().writeBytes(bytes)
        return outPath
    }

    suspend fun putObject(k: Path, buf: ByteBuffer) {
        buffers[k] = buf
        calls.add(StoreOperation.PUT)
    }

    fun listAllObjects() = buffers.map { (key, buffer) -> StoredObject(key, buffer.capacity().toLong()) }

    fun listAllObjects(dir: Path): List<StoredObject> =
        buffers.tailMap(dir).entries
            .takeWhile { it.key.startsWith(dir) }
            .map { (key, buffer) -> StoredObject(key, buffer.capacity().toLong()) }

    fun listAfter(dir: Path, afterKey: Path): List<StoredObject> =
        buffers.tailMap(afterKey, false).entries
            .takeWhile { it.key.startsWith(dir) }
            .map { (key, buffer) -> StoredObject(key, buffer.capacity().toLong()) }

    suspend fun copyObject(src: Path, dest: Path) {
        val srcBuffer = buffers[src] ?: throw IllegalStateException("Object $src doesn't exist")
        buffers[dest] = copyByteBuffer(srcBuffer)
    }

    suspend fun deleteIfExists(k: Path) {
        buffers.remove(k)
    }

    suspend fun startMultipart(k: Path): IMultipartUpload<ByteBuffer> =
        object : IMultipartUpload<ByteBuffer> {
            override suspend fun uploadPart(idx: Int, buf: ByteBuffer): ByteBuffer {
                calls.add(StoreOperation.UPLOAD)
                return copyByteBuffer(buf)
            }

            override suspend fun complete(parts: List<ByteBuffer>) {
                calls.add(StoreOperation.COMPLETE)
                buffers[k] = concatByteBuffers(parts)
            }

            override suspend fun abort() {
                calls.add(StoreOperation.ABORT)
            }
        }
}
