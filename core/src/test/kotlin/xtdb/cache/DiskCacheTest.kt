package xtdb.cache

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText

class DiskCacheTest {

    @Test
    fun `startup deletes leftover staging files rather than adopting them`(@TempDir rootPath: Path) {
        val leftover = rootPath.resolve(".tmp").createDirectories().resolve("upload1.arrow").also { it.writeText("partial") }
        rootPath.resolve("c1/abc/blocks").createDirectories().resolve("b00.binpb").writeText("block")

        val cache = DiskCache.Factory(rootPath).build()

        assertEquals(setOf(Path.of("c1/abc/blocks/b00.binpb")), cache.pinningCache.cache.asMap().keys)
        assertFalse(leftover.exists())
    }

    @Test
    fun `staging file is cleaned up when fetch fails`(@TempDir rootPath: Path) {
        val cache = DiskCache.Factory(rootPath).build()
        val stagingDir = rootPath.resolve(".tmp")

        val future = cache.get(Path.of("missing-key")) { _, _ ->
            CompletableFuture.failedFuture(RuntimeException("simulated fetch failure"))
        }

        assertThrows<ExecutionException> { future.get() }

        assertEquals(
            emptyList<Path>(), stagingDir.listDirectoryEntries(),
            "staging files should be cleaned up after fetch failure"
        )
    }
}
