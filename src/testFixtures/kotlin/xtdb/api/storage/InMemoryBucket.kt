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

    fun getObject(k: Path): CompletableFuture<ByteBuffer> =
        completedFuture(buffers[k])

    fun getObject(k: Path, outPath: Path): CompletableFuture<Path> =
        buffers[k]?.let { buffer ->
            val bytes = ByteArray(buffer.remaining())
            buffer.duplicate().get(bytes)
            outPath.toFile().writeBytes(bytes)
            completedFuture(outPath)
        } ?: failedFuture(IllegalStateException("Object $k doesn't exist"))

    fun putObject(k: Path, buf: ByteBuffer): CompletableFuture<Unit> {
        buffers[k] = buf
        calls.add(StoreOperation.PUT)
        return completedFuture(Unit)
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

    fun copyObject(src: Path, dest: Path): CompletableFuture<Unit> {
        val srcBuffer = buffers[src] ?: return failedFuture(IllegalStateException("Object $src doesn't exist"))
        buffers[dest] = copyByteBuffer(srcBuffer)
        return completedFuture(Unit)
    }

    fun deleteIfExists(k: Path): CompletableFuture<Unit> {
        buffers.remove(k)
        return completedFuture(Unit)
    }

    fun startMultipart(k: Path): CompletableFuture<IMultipartUpload<ByteBuffer>> {
        val upload = object : IMultipartUpload<ByteBuffer> {
            override fun uploadPart(idx: Int, buf: ByteBuffer): CompletableFuture<ByteBuffer> {
                calls.add(StoreOperation.UPLOAD)
                return completedFuture(copyByteBuffer(buf))
            }

            override fun complete(parts: List<ByteBuffer>): CompletableFuture<Unit> {
                calls.add(StoreOperation.COMPLETE)
                buffers[k] = concatByteBuffers(parts)
                return completedFuture(Unit)
            }

            override fun abort(): CompletableFuture<Unit> {
                calls.add(StoreOperation.ABORT)
                return completedFuture(Unit)
            }
        }

        return completedFuture(upload)
    }
}
