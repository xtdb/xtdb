package xtdb.catalog

import kotlinx.coroutines.test.runTest
import org.apache.arrow.memory.BufferAllocator
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import xtdb.api.storage.Storage
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.cache.MemoryCache
import xtdb.catalog.BlockCatalog.Companion.latestBlock
import xtdb.garbage_collector.BlockGarbageCollector
import xtdb.storage.BufferPool
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
class BlockGarbageCollectorTest {
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
    fun `garbageCollectBlocks deletes oldest blocks and per-table blocks`(@TempDir tempDir: Path, al: BufferAllocator) = runTest {
        MemoryCache.Factory().open(al).use { memoryCache ->
            Storage.local(tempDir).open(al, memoryCache, null, "xtdb").use { bufferPool ->
                // Write dummy blocks and table blocks
                val blocksPath = "blocks".asPath
                val table1BlockPath = "public/foo".tablePath.resolve(blocksPath)
                val table2BlockPath = "public/bar".tablePath.resolve(blocksPath)

                (1L..10L).forEach { index ->
                    writeBlock(
                        bufferPool, index, listOf("public/foo", "public/bar"),
                        listOf(blocksPath, table1BlockPath, table2BlockPath)
                    )
                }

                assertBlockCount(bufferPool, blocksPath, 10)
                assertBlockCount(bufferPool, table1BlockPath, 10)
                assertBlockCount(bufferPool, table2BlockPath, 10)

                val blockCat = BlockCatalog("xtdb", bufferPool.latestBlock)

                val gc = BlockGarbageCollector(
                    bufferPool, blockCat,
                    blocksToKeep = 3,
                    enabled = false,
                    dbName = "xtdb",
                )

                // Validate that the latest block is correct
                val latestBlockIndex = blockCat.currentBlockIndex
                assertEquals(10L, latestBlockIndex)

                val latestCompletedTx = blockCat.latestCompletedTx
                assertEquals(10L, latestCompletedTx?.txId)

                gc.garbageCollectBlocks()

                // Validate that the oldest blocks are deleted, leaving only the latest 3 blocks
                assertBlockCount(bufferPool, blocksPath, 3)
                assertBlockCount(bufferPool, table1BlockPath, 3)
                assertBlockCount(bufferPool, table2BlockPath, 3)

                // Validate latest block still exists
                val latestBlockFile = bufferPool.listAllObjects(blocksPath).toList()
                    .find { it.key == blocksPath.resolve("b${10L.asLexHex}.binpb") }
                assertNotNull(latestBlockFile, "Expected latest block file to exist after GC, but it was deleted")
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
