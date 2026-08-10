package xtdb.api.storage

import kotlinx.coroutines.runBlocking
import xtdb.multipart.IMultipartUpload
import xtdb.multipart.SupportsMultipart
import xtdb.multipart.SupportsMultipart.Companion.uploadMultipartBuffers
import java.nio.ByteBuffer
import java.nio.file.Path

/**
 * Blocking bridges onto the suspending halves of [ObjectStore] and [SupportsMultipart], for Clojure tests.
 *
 * Clojure interop cannot call a suspend function — its JVM signature carries a hidden `Continuation`
 * parameter that `.method` interop has no way to supply. These live in test fixtures rather than on the
 * interfaces themselves so that production code has no blocking entry point into object-store I/O; that
 * is the whole point of #5857, and a bridge on the interface would be an open invitation to undo it.
 *
 * Kotlin callers must not use these — suspend directly instead.
 */
object ObjectStoreSync {

    @JvmStatic
    fun getObject(os: ObjectStore, k: Path): ByteBuffer = runBlocking { os.getObject(k) }

    @JvmStatic
    fun getObject(os: ObjectStore, k: Path, outPath: Path): Path = runBlocking { os.getObject(k, outPath) }

    @JvmStatic
    fun putObject(os: ObjectStore, k: Path, buf: ByteBuffer) = runBlocking { os.putObject(k, buf) }

    @JvmStatic
    fun copyObject(os: ObjectStore, src: Path, dest: Path) = runBlocking { os.copyObject(src, dest) }

    @JvmStatic
    fun deleteIfExists(os: ObjectStore, k: Path) = runBlocking { os.deleteIfExists(k) }

    @JvmStatic
    fun <P> startMultipart(os: SupportsMultipart<P>, k: Path): IMultipartUpload<P> =
        runBlocking { os.startMultipart(k) }

    @JvmStatic
    fun <P> uploadPart(upload: IMultipartUpload<P>, idx: Int, buf: ByteBuffer): P =
        runBlocking { upload.uploadPart(idx, buf) }

    @JvmStatic
    fun <P> complete(upload: IMultipartUpload<P>, parts: List<P>) = runBlocking { upload.complete(parts) }

    @JvmStatic
    fun <P> abort(upload: IMultipartUpload<P>) = runBlocking { upload.abort() }

    @JvmStatic
    fun <P> uploadMultipartBuffers(os: SupportsMultipart<P>, k: Path, buffers: List<ByteBuffer>) =
        runBlocking { os.uploadMultipartBuffers(k, buffers) }
}
