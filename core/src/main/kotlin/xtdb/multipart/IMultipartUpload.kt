package xtdb.multipart

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

interface IMultipartUpload<Part> {

    /**
     * Asynchronously uploads a part to the multipart request and adds it to the internal list of completed parts.
     */
    fun uploadPart(buf: ByteBuffer): CompletableFuture<Part>

    /**
     * Asynchronously completes the multipart request.
     */
    fun complete(parts: List<Part>): CompletableFuture<Unit>

    /**
     * Asynchronously cancels the multipart request, useful for cleaning up any parts of the multipart upload in case of an error.
     */
    fun abort(): CompletableFuture<Unit>
}
