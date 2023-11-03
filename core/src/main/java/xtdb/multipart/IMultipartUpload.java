package xtdb.multipart;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface IMultipartUpload {
    /**
     * Asynchronously uploads a part to the multipart request and adds it to the internal list of completed parts.
     */
    CompletableFuture<Void> uploadPart(ByteBuffer buf);

    /**
     * Asynchronously completes the multipart request.
     */
    CompletableFuture<Void> complete();

    /**
     * Asynchronously cancels the multipart request, useful for cleaning up any parts of the multipart upload in case of an error.
     */
    CompletableFuture<?> abort();
}
