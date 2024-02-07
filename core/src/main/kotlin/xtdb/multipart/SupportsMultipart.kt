package xtdb.multipart

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface SupportsMultipart {
    fun startMultipart(k: Path): CompletableFuture<IMultipartUpload>
}
