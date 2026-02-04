package xtdb.garbage_collector

import org.slf4j.LoggerFactory
import xtdb.catalog.BlockCatalog
import xtdb.catalog.BlockCatalog.Companion.allBlockFiles
import xtdb.catalog.BlockCatalog.Companion.tableBlocks
import xtdb.storage.BufferPool
import xtdb.util.StringUtil.fromLexHex
import java.nio.file.Path

private val LOGGER = LoggerFactory.getLogger(BlockGarbageCollector::class.java)

class BlockGarbageCollector(
    private val blockCatalog: BlockCatalog,
    private val bufferPool: BufferPool,
    private val blocksToKeep: Int,
) {

    private fun Path.parseBlockIndex(): Long? =
        Regex("b(\\p{XDigit}+)\\.binpb")
            .matchEntire(fileName.toString())
            ?.groups?.get(1)
            ?.value?.fromLexHex

    private fun deleteBlock(blockPath: Path) {
        check(blockPath.parseBlockIndex() != blockCatalog.currentBlockIndex) {
            "Cannot delete current block $blockPath - aborting"
        }
        LOGGER.debug("Deleting block file {}", blockPath)
        bufferPool.deleteIfExists(blockPath)
    }

    @Suppress("LoggingSimilarMessage")
    fun garbageCollectBlocks(blocksToKeep: Int = this.blocksToKeep) {
        LOGGER.debug("Garbage collecting blocks, keeping $blocksToKeep blocks")

        bufferPool.allBlockFiles.toList()
            .dropLast(blocksToKeep)
            .let { blocks ->
                LOGGER.debug("Deleting oldest ${blocks.size} block files")
                blocks.forEach { deleteBlock(it.key) }
            }

        blockCatalog.allTables
            .shuffled().take(100)
            .forEach { table ->
                LOGGER.debug("Garbage collecting blocks for table {}, keeping {} blocks", table.sym, blocksToKeep)
                bufferPool.tableBlocks(table).toList()
                    .dropLast(blocksToKeep)
                    .let { blocks ->
                        LOGGER.debug("Deleting oldest ${blocks.size} table block files")
                        blocks.forEach { deleteBlock(it.key) }
                    }
            }
    }
}