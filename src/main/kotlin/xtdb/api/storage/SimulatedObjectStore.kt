package xtdb.api.storage

import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.CompletableFuture.failedFuture

enum class StoreOperation {
    PUT, UPLOAD, COMPLETE, ABORT
}

class SimulatedObjectStore(
    val calls: MutableList<StoreOperation> = mutableListOf(),
    val buffers: NavigableMap<Path, ByteBuffer> = TreeMap()
) : ObjectStore, SupportsMultipart<ByteBuffer> {

    private fun copyByteBuffer(buffer: ByteBuffer) =
        ByteBuffer.allocate(buffer.remaining()).put(buffer.duplicate()).flip()

    private fun concatByteBuffers(parts: List<ByteBuffer>): ByteBuffer {
        val totalSize = parts.sumOf { it.remaining() }
        val buffer = ByteBuffer.allocate(totalSize)
        parts.forEach { buffer.put(it.duplicate()) }
        return buffer.flip()
    }

    override fun getObject(k: Path): CompletableFuture<ByteBuffer> =
        completedFuture(buffers[k])

    override fun getObject(k: Path, outPath: Path): CompletableFuture<Path> =
        buffers[k]?.let { buffer ->
            val bytes = ByteArray(buffer.remaining())
            buffer.duplicate().get(bytes)
            outPath.toFile().writeBytes(bytes)
            completedFuture(outPath)
        } ?: failedFuture(IllegalStateException("Object $k doesn't exist"))

    override fun putObject(k: Path, buf: ByteBuffer): CompletableFuture<Unit> {
        buffers[k] = buf
        calls.add(StoreOperation.PUT)
        return completedFuture(Unit)
    }

    override fun listAllObjects() = buffers.map { (key, buffer) -> StoredObject(key, buffer.capacity().toLong()) }

    override fun listAllObjects(dir: Path): List<StoredObject> =
        buffers.tailMap(dir).entries
            .takeWhile { it.key.startsWith(dir) }
            .map { (key, buffer) -> StoredObject(key, buffer.capacity().toLong()) }

    override fun deleteObject(k: Path): CompletableFuture<Unit> =
        TODO("Not yet implemented")

    override fun startMultipart(k: Path): CompletableFuture<IMultipartUpload<ByteBuffer>> {
        val upload = object : IMultipartUpload<ByteBuffer> {
            override fun uploadPart(buf: ByteBuffer): CompletableFuture<ByteBuffer> {
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