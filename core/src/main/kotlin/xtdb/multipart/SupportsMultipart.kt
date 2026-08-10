package xtdb.multipart

import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import xtdb.api.storage.ObjectStore
import xtdb.storage.RemoteBufferPool
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.nio.file.Path

interface SupportsMultipart<Part> : ObjectStore {
    suspend fun startMultipart(k: Path): IMultipartUpload<Part>

    companion object {

        private const val MAX_CONCURRENT_PART_UPLOADS = 4

        private val LOGGER = RemoteBufferPool::class.logger

        // bounds the number of parts in flight, and hence the buffers pinned for them. Note that a
        // `limitedParallelism` view of `Dispatchers.IO` is *elastic* — it draws on an unbounded pool
        // rather than IO's own permits — so this caps concurrency, not thread supply.
        private val multipartUploadDispatcher =
            IO.limitedParallelism(MAX_CONCURRENT_PART_UPLOADS, "upload-multipart")

        suspend fun <P> SupportsMultipart<P>.uploadMultipartBuffers(key: Path, nioBuffers: List<ByteBuffer>): Unit =
            coroutineScope {
                val upload = startMultipart(key)

                try {
                    val waitingParts = nioBuffers.mapIndexed { idx, it ->
                        async(multipartUploadDispatcher) { upload.uploadPart(idx, it) }
                    }

                    upload.complete(waitingParts.awaitAll())
                } catch (e: Throwable) {
                    // NonCancellable because the common reason for getting here is that our scope was
                    // cancelled — a request timeout, say. Aborting is itself a suspending call to the
                    // store, so without this it would fail immediately and leave the parts orphaned.
                    withContext(NonCancellable) {
                        try {
                            LOGGER.warn("Error caught in uploadMultipartBuffers - aborting multipart upload of $key")
                            upload.abort()
                        } catch (abortError: Throwable) {
                            LOGGER.warn(abortError, "Throwable caught when aborting uploadMultipartBuffers")
                            e.addSuppressed(abortError)
                        }
                    }
                    throw e
                }
            }
    }
}
