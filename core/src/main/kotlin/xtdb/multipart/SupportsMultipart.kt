package xtdb.multipart

import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import xtdb.api.storage.ObjectStore
import xtdb.buffer_pool.RemoteBufferPool
import xtdb.util.logger
import xtdb.util.warn
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface SupportsMultipart<Part> : ObjectStore {
    fun startMultipart(k: Path): CompletableFuture<IMultipartUpload<Part>>

    companion object {

        private const val MAX_CONCURRENT_PART_UPLOADS = 4

        private val LOGGER = RemoteBufferPool::class.logger

        private val multipartUploadDispatcher =
            IO.limitedParallelism(MAX_CONCURRENT_PART_UPLOADS, "upload-multipart")

        @JvmStatic
        fun <P> SupportsMultipart<P>.uploadMultipartBuffers(key: Path, nioBuffers: List<ByteBuffer>): Unit = runBlocking {
            val upload = startMultipart(key).await()

            try {
                val waitingParts = nioBuffers.mapIndexed { idx, it ->
                    async(multipartUploadDispatcher) {
                        upload.uploadPart(idx, it).await()
                    }
                }

                upload.complete(waitingParts.awaitAll()).await()
            } catch (e: Throwable) {
                try {
                    LOGGER.warn("Error caught in uploadMultipartBuffers - aborting multipart upload of $key")
                    upload.abort().get()
                } catch (abortError: Throwable) {
                    LOGGER.warn(abortError, "Throwable caught when aborting uploadMultipartBuffers")
                    e.addSuppressed(abortError)
                }
                throw e
            }
        }
    }
}
