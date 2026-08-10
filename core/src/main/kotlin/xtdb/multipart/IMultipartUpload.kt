package xtdb.multipart

import java.nio.ByteBuffer

interface IMultipartUpload<Part> {

    /**
     * Uploads a part to the multipart request and adds it to the internal list of completed parts.
     */
    suspend fun uploadPart(idx: Int, buf: ByteBuffer): Part

    /**
     * Completes the multipart request.
     */
    suspend fun complete(parts: List<Part>)

    /**
     * Cancels the multipart request, useful for cleaning up any parts of the multipart upload in case of an error.
     */
    suspend fun abort()
}
