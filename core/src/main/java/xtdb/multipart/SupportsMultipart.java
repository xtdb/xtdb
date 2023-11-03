package xtdb.multipart;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface SupportsMultipart {
    CompletableFuture<IMultipartUpload> startMultipart(Path k);
}
