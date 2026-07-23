package xtdb.api.storage

import xtdb.api.storage.ObjectStore.StoredObject
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

/**
 * The test-fixture analogue of a production [ObjectStore] client (`xtdb.aws.S3` et al): it resolves
 * pool-relative keys under [prefix] on the way in and relativizes them back out, exactly as the
 * cloud stores do internally with their configured prefix + storage root. The [delegate] plays the
 * durable bucket behind it.
 *
 * The point of the split is that one [delegate] can back several pools opened at different prefixes
 * (per-partition pools, or successive opens of the same store), so a test that shares one bucket can
 * assert cross-pool isolation and read the bucket's raw key-space — neither of which is observable
 * when a pool talks straight to a bucket.
 *
 * Closing is a no-op: the delegate is shared, owned by whoever created it, and outlives any one pool.
 */
class PrefixedObjectStore(
    private val prefix: Path, private val delegate: SimulatedObjectStore,
) : SupportsMultipart<ByteBuffer> {

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
