package xtdb.api.storage

import xtdb.api.storage.ObjectStore.Companion.throwMissingKey
import xtdb.api.storage.ObjectStore.StoredObject
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import java.util.NavigableMap
import java.util.concurrent.ConcurrentSkipListMap

/**
 * A whole [ObjectStore] backed by a map, for exercising the object-store contract without a cloud account.
 *
 * Writes are put-if-absent, so a test that re-puts a key asserts the *first* value survives. That is the
 * one behaviour separating this from [InMemoryBucket], whose writes overwrite.
 *
 * Kotlin rather than a Clojure `deftype` because the write methods suspend, and Clojure interop cannot
 * implement a suspend method — its JVM signature carries a hidden `Continuation` parameter.
 */
class InMemoryObjectStore(
    private val objects: NavigableMap<Path, ByteBuffer> = ConcurrentSkipListMap()
) : ObjectStore {

    override suspend fun getObject(k: Path): ByteBuffer = (objects[k] ?: throwMissingKey(k)).slice()

    override suspend fun getObject(k: Path, outPath: Path): Path {
        val buf = objects[k] ?: throwMissingKey(k)
        FileChannel.open(outPath, WRITE, TRUNCATE_EXISTING, CREATE).use { it.write(buf.slice()) }
        return outPath
    }

    override suspend fun putObject(k: Path, buf: ByteBuffer) {
        objects.putIfAbsent(k, buf.slice())
    }

    override fun listAllObjects(): Iterable<StoredObject> =
        objects.map { (k, buf) -> StoredObject(k, buf.capacity().toLong()) }

    override fun listAllObjects(dir: Path): Iterable<StoredObject> =
        objects.tailMap(dir).entries
            .takeWhile { it.key.startsWith(dir) }
            .map { (k, buf) -> StoredObject(k, buf.capacity().toLong()) }

    override suspend fun copyObject(src: Path, dest: Path) {
        objects.putIfAbsent(dest, (objects[src] ?: throwMissingKey(src)).slice())
    }

    override suspend fun deleteIfExists(k: Path) {
        objects.remove(k)
    }

    override fun close() = objects.clear()
}
