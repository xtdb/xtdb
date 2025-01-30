package xtdb.api.storage

import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.*
import java.util.concurrent.CompletableFuture

enum class StoreOperation {
    PUT, UPLOAD, COMPLETE, ABORT
}

class SimulatedObjectStore(
    val calls: MutableList<StoreOperation> = mutableListOf(),
    val buffers: NavigableMap<Path, ByteBuffer> = TreeMap()
) : ObjectStore, SupportsMultipart {

    private fun copyByteBuffer(buffer: ByteBuffer) =
        ByteBuffer.allocate(buffer.remaining()).put(buffer.duplicate()).flip()

    private fun concatByteBuffers(parts: List<ByteBuffer>): ByteBuffer {
        val totalSize = parts.sumOf { it.remaining() }
        val buffer = ByteBuffer.allocate(totalSize)
        parts.forEach { buffer.put(it.duplicate()) }
        return buffer.flip()
    }

    override fun getObject(k: Path): CompletableFuture<ByteBuffer> =
        CompletableFuture.completedFuture(buffers[k])

    override fun getObject(k: Path, outPath: Path): CompletableFuture<Path> =
        buffers[k]?.let { buffer ->
            val bytes = ByteArray(buffer.remaining())
            buffer.duplicate().get(bytes)
            outPath.toFile().writeBytes(bytes)
            CompletableFuture.completedFuture(outPath)
        } ?: CompletableFuture.failedFuture(IllegalStateException("Object $k doesn't exist"))

    override fun putObject(k: Path, buf: ByteBuffer): CompletableFuture<Void?> {
        buffers[k] = buf
        calls.add(StoreOperation.PUT)
        return CompletableFuture.completedFuture(null)
    }

    override fun listObjects() = buffers.map { (key, buffer) -> StoredObject(key, buffer.capacity().toLong()) }

    override fun listObjects(dir: Path): List<StoredObject> =
        buffers.tailMap(dir).entries
            .takeWhile { it.key.startsWith(dir) }
            .map { (key, buffer) -> StoredObject(key, buffer.capacity().toLong()) }

    override fun deleteObject(k: Path): CompletableFuture<*> =
        TODO("Not yet implemented")

    override fun startMultipart(k: Path): CompletableFuture<IMultipartUpload> {
        val parts = mutableListOf<ByteBuffer>()

        val upload = object : IMultipartUpload {
            override fun uploadPart(buf: ByteBuffer): CompletableFuture<Void?> {
                calls.add(StoreOperation.UPLOAD)
                parts.add(copyByteBuffer(buf))
                return CompletableFuture.completedFuture(null)
            }

            override fun complete(): CompletableFuture<Void?> {
                calls.add(StoreOperation.COMPLETE)
                buffers[k] = concatByteBuffers(parts)
                return CompletableFuture.completedFuture(null)
            }

            override fun abort(): CompletableFuture<Void?> {
                calls.add(StoreOperation.ABORT)
                return CompletableFuture.completedFuture(null)
            }
        }

        return CompletableFuture.completedFuture(upload)
    }
}