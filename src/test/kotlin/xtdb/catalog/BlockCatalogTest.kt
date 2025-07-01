package xtdb.catalog

import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import xtdb.BufferPool
import xtdb.api.storage.Storage
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.cache.MemoryCache
import xtdb.test.AllocatorResolver
import xtdb.time.InstantUtil.asMicros
import xtdb.trie.Trie.tablePath
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.Instant
import kotlin.test.assertEquals

@ExtendWith(AllocatorResolver::class)
class BlockCatalogTest {
    private fun writeBlock(bufferPool: BufferPool, index: Long, tableNames: List<String>, basePaths: List<Path>) {
        val block = block {
            blockIndex = index
            latestCompletedTx = txKey {
                txId = index
                systemTime = Instant.now().asMicros
            }
            this.tableNames.addAll(tableNames)
        }
        val bytes = ByteBuffer.wrap(block.toByteArray())
        basePaths.forEach { bufferPool.putObject(it.resolve("b${index.asLexHex}.binpb"), bytes.duplicate()) }
    }

    private fun assertBlockCount(bufferPool: BufferPool, path: Path, expected: Int) {
        val files = bufferPool.listAllObjects(path).toList()
        assertEquals(expected, files.size, "Expected $expected block files at $path, but got ${files.size}")
    }

    @Test
    fun `garbageCollectBlocks deletes oldest blocks and per-table blocks`(@TempDir tempDir: Path, al: BufferAllocator) {
        MemoryCache.Factory().open(al).use { memoryCache ->
            Storage.localStorage(tempDir).open(al, memoryCache, null).use { bufferPool ->
                // Write dummy blocks and table blocks
                val blocksPath = "blocks".asPath
                val table1BlockPath = "foo".tablePath.resolve(blocksPath)
                val table2BlockPath = "bar".tablePath.resolve(blocksPath)

                // Write 10 blocks
                (1L..10L).forEach { index ->
                    writeBlock(
                        bufferPool, index, listOf("foo", "bar"),
                        listOf(blocksPath, table1BlockPath, table2BlockPath)
                    )
                }

                // Validate 10 files exist for blocks and table blocks
                assertBlockCount(bufferPool, blocksPath, 10)
                assertBlockCount(bufferPool, table1BlockPath, 10)
                assertBlockCount(bufferPool, table2BlockPath, 10)

                // Create BlockCatalog instance
                val catalog = BlockCatalog(bufferPool)

                // Validate that the latest block is correct
                val latestBlockIndex = catalog.currentBlockIndex
                assertEquals(10L, latestBlockIndex)

                // Validate latest completed transaction
                val latestCompletedTx = catalog.latestCompletedTx
                assertEquals(10L, latestCompletedTx?.txId)

                // Trigger block GC
                catalog.garbageCollectBlocks(blocksToKeep = 3)

                // Validate that the oldest blocks are deleted, leaving only the latest 3 blocks
                assertBlockCount(bufferPool, blocksPath, 3)
                assertBlockCount(bufferPool, table1BlockPath, 3)
                assertBlockCount(bufferPool, table2BlockPath, 3)

                // Validate latest block still exists
                val latestBlockFile = bufferPool.listAllObjects(blocksPath).toList()
                    .find { it.key == blocksPath.resolve("b${10L.asLexHex}.binpb") }
                assertNotNull(latestBlockFile, "Expected latest block file to exist after GC, but it was deleted")

                // Attempting to delete the current block should throw an exception
                assertThrows<IllegalStateException> {
                    catalog.garbageCollectBlocks(blocksToKeep = 0)
                }

                // Latest block should still exist - in blocks and table blocks
                assertBlockCount(bufferPool, blocksPath, 1)

                val latestBlockFileAfterGC = bufferPool.listAllObjects(blocksPath).toList()
                    .find { it.key == blocksPath.resolve("b${10L.asLexHex}.binpb") }
                val latestTable1BlockFileAfterGC = bufferPool.listAllObjects(table1BlockPath).toList()
                    .find { it.key == table1BlockPath.resolve("b${10L.asLexHex}.binpb") }
                val latestTable2BlockFileAfterGC = bufferPool.listAllObjects(table2BlockPath).toList()
                    .find { it.key == table2BlockPath.resolve("b${10L.asLexHex}.binpb") }

                assertNotNull(
                    latestBlockFileAfterGC,
                    "Expected latest block file to exist after GC, but it was deleted"
                )
                assertNotNull(
                    latestTable1BlockFileAfterGC,
                    "Expected latest table1 block file to exist after GC, but it was deleted"
                )
                assertNotNull(
                    latestTable2BlockFileAfterGC,
                    "Expected latest table2 block file to exist after GC, but it was deleted"
                )
            }

        }
    }

    /*
    TODO: Fix this test - race condition with ListAllObjects
    @Test
    fun `garbageCollectBlocks handles concurrent deletions gracefully`(@TempDir tempDir: Path) = runBlocking {
        val bufferPool: BufferPool = LocalBufferPool(
            Storage.localStorage(tempDir),
            Storage.VERSION,
            RootAllocator(),
            SimpleMeterRegistry()
        )

        val blocksPath = "blocks".asPath
        val tableBlockPath = "foo".tablePath.resolve(blocksPath)

        (1L..50000L).forEach { index ->
            writeBlock(bufferPool, index, listOf("foo"), listOf(blocksPath, tableBlockPath))
        }

        val allBlockKeys = bufferPool.listAllObjects(blocksPath).map { it.key }
        val allTableBlockKeys = bufferPool.listAllObjects(tableBlockPath).map { it.key }

        val catalog = BlockCatalog(bufferPool)

        coroutineScope {
            launch(Dispatchers.IO) {
                catalog.garbageCollectBlocks(blocksToKeep = 3)
            }

            launch(Dispatchers.IO) {
                delay(100) // Ensure GC starts first
                (20000 .. 40000 step 2).forEach { index ->
                    bufferPool.deleteIfExists(allBlockKeys[index])
                    bufferPool.deleteIfExists(allTableBlockKeys[index])
                }
            }
        }.join()

        assertBlockCount(bufferPool, blocksPath, 3)
    }*/
}
