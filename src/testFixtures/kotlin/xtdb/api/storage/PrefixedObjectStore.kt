package xtdb.api.storage

import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

/**
 * Scopes a delegate [ObjectStore] under [prefix], mirroring how the production stores
 * (S3/Azure/GCS) resolve their configured prefix + storage root inside the implementation —
 * including multipart uploads, so pools opened over this take the same upload paths they would
 * against a real store.
 *
 * Lets tests share one backing store across several `openObjectStore` calls — e.g. per-partition
 * BufferPools over a single simulated store — and then assert on the delegate's raw key-space.
 *
 * Closing is a no-op: the shared delegate belongs to whoever created it.
 */
class PrefixedObjectStore<D>(
    private val prefix: Path, private val delegate: D,
) : SupportsMultipart<ByteBuffer> where D : ObjectStore, D : SupportsMultipart<ByteBuffer> {

    private fun StoredObject.relativized() = StoredObject(prefix.relativize(key), size)

    override fun getObject(k: Path): CompletableFuture<ByteBuffer> = delegate.getObject(prefix.resolve(k))

    override fun getObject(k: Path, outPath: Path): CompletableFuture<Path> =
        delegate.getObject(prefix.resolve(k), outPath)

    override fun putObject(k: Path, buf: ByteBuffer): CompletableFuture<Unit> =
        delegate.putObject(prefix.resolve(k), buf)

    override fun startMultipart(k: Path): CompletableFuture<IMultipartUpload<ByteBuffer>> =
        delegate.startMultipart(prefix.resolve(k))

    override fun listAllObjects(): Iterable<StoredObject> =
        delegate.listAllObjects(prefix).map { it.relativized() }

    override fun listAllObjects(dir: Path): Iterable<StoredObject> =
        delegate.listAllObjects(prefix.resolve(dir)).map { it.relativized() }

    override fun listAfter(dir: Path, afterKey: Path): Iterable<StoredObject> =
        delegate.listAfter(prefix.resolve(dir), prefix.resolve(afterKey)).map { it.relativized() }

    override fun copyObject(src: Path, dest: Path): CompletableFuture<Unit> =
        delegate.copyObject(prefix.resolve(src), prefix.resolve(dest))

    override fun deleteIfExists(k: Path): CompletableFuture<Unit> = delegate.deleteIfExists(prefix.resolve(k))

    override fun close() {}
}
