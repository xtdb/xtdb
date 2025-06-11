package xtdb.catalog

import org.slf4j.LoggerFactory
import xtdb.BufferPool
import xtdb.api.TransactionKey
import xtdb.block.proto.Block
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.BlockIndex
import xtdb.trie.TableName
import xtdb.trie.Trie.tablePath
import xtdb.util.StringUtil.asLexHex
import xtdb.util.StringUtil.fromLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path

private val LOGGER = LoggerFactory.getLogger(BlockCatalog::class.java)

class BlockCatalog(private val bp: BufferPool) {

    @Volatile
    private var latestBlock: Block? =
        bp.listAllObjects(blocksPath).lastOrNull()?.key
            ?.let { blockKey -> Block.parseFrom(bp.getByteArray(blockKey)) }

    fun blockFromLatest(distance: Int): Block? =
        bp.listAllObjects(blocksPath).toList().dropLast(maxOf(0, distance-1)).lastOrNull()?.key
            ?.let { blockKey -> Block.parseFrom(bp.getByteArray(blockKey)) }

    companion object {
        private val blocksPath = "blocks".asPath
    }

    fun finishBlock(blockIndex: BlockIndex, latestCompletedTx: TransactionKey, tableNames: Collection<TableName>) {
        val currentBlockIndex = this.currentBlockIndex
        check(currentBlockIndex == null || currentBlockIndex < blockIndex) {
            "Cannot finish block $blockIndex when current block is $currentBlockIndex"
        }

        val newBlock = block {
            this.blockIndex = blockIndex
            this.latestCompletedTx = txKey {
                txId = latestCompletedTx.txId
                systemTime = latestCompletedTx.systemTime.asMicros
            }
            this.tableNames.addAll(tableNames)
        }

        bp.putObject(blocksPath.resolve("b${blockIndex.asLexHex}.binpb"), ByteBuffer.wrap(newBlock.toByteArray()))

        this.latestBlock = newBlock
    }

    val currentBlockIndex get() = latestBlock?.blockIndex

    val latestCompletedTx: TransactionKey?
        get() = latestBlock?.latestCompletedTx
            ?.let { TransactionKey(it.txId, it.systemTime.microsAsInstant) }

    val allTableNames: List<TableName> get() = latestBlock?.tableNamesList.orEmpty()

    private fun Path.parseBlockIndex(): Long? =
        Regex("b(\\p{XDigit}+)\\.binpb")
            .matchEntire(fileName.toString())
            ?.groups?.get(1)
            ?.value?.fromLexHex

    private fun deleteBlock(blockPath: Path) {
        check(blockPath.parseBlockIndex() != currentBlockIndex) {
            "Cannot delete current block $blockPath - aborting"
        }
        LOGGER.debug("Deleting block file $blockPath")
        bp.deleteIfExists(blockPath)
    }

    private fun deleteOldestBlocks(path: Path, blocksToKeep: Int) {
        val blocks = bp.listAllObjects(path).toList().dropLast(blocksToKeep)
        LOGGER.debug("Deleting oldest ${blocks.size} block files")
        blocks.forEach { block -> deleteBlock(block.key) }
    }

    fun garbageCollectBlocks(blocksToKeep: Int) {
        LOGGER.debug("Garbage collecting blocks, keeping $blocksToKeep blocks")
        deleteOldestBlocks(blocksPath, blocksToKeep)

        allTableNames.shuffled().take(100).forEach { tableName ->
            LOGGER.debug("Garbage collecting blocks for table $tableName, keeping $blocksToKeep blocks")
            val tableBlockPath = tableName.tablePath.resolve(blocksPath)
            deleteOldestBlocks(tableBlockPath, blocksToKeep)
        }
    }
}