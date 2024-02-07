package xtdb.multipart

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

interface IMultipartUpload {
    /**
     * Asynchronously uploads a part to the multipart request and adds it to the internal list of completed parts.
     */
    fun uploadPart(buf: ByteBuffer): CompletableFuture<*>

    /**
     * Asynchronously completes the multipart request.
     */
    fun complete(): CompletableFuture<*>

    /**
     * Asynchronously cancels the multipart request, useful for cleaning up any parts of the multipart upload in case of an error.
     */
    fun abort(): CompletableFuture<*>
}
