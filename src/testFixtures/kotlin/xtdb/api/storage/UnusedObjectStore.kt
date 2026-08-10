package xtdb.api.storage

import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.storage.ObjectStore.StoredObject
import java.nio.ByteBuffer
import java.nio.file.Path
import com.google.protobuf.Any as ProtoAny

/**
 * An [ObjectStore] that throws on every operation, for tests asserting the cache layers serve a read
 * without reaching the store at all — the throw is the assertion.
 *
 * Kotlin rather than a Clojure `reify` because the write methods suspend, and Clojure interop cannot
 * implement a suspend method — its JVM signature carries a hidden `Continuation` parameter.
 */
object UnusedObjectStore : ObjectStore {

    private fun unused(op: String): Nothing = throw UnsupportedOperationException(op)

    override suspend fun getObject(k: Path): ByteBuffer = unused("getObject")
    override suspend fun getObject(k: Path, outPath: Path): Path = unused("getObject")
    override suspend fun putObject(k: Path, buf: ByteBuffer) = unused("putObject")
    override fun listAllObjects(): Iterable<StoredObject> = unused("listAllObjects")
    override fun listAllObjects(dir: Path): Iterable<StoredObject> = unused("listAllObjects")
    override suspend fun copyObject(src: Path, dest: Path) = unused("copyObject")
    override suspend fun deleteIfExists(k: Path) = unused("deleteIfExists")

    object Factory : ObjectStore.Factory {
        override fun openObjectStore(storageRoot: Path, remotes: Map<RemoteAlias, Remote>): ObjectStore =
            UnusedObjectStore

        override val configProto: ProtoAny get() = ProtoAny.newBuilder().build()
    }
}
