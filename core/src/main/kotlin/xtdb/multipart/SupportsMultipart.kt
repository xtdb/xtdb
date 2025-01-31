package xtdb.multipart

import xtdb.api.storage.ObjectStore
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

interface SupportsMultipart<Part> : ObjectStore {
    fun startMultipart(k: Path): CompletableFuture<IMultipartUpload<Part>>
}
