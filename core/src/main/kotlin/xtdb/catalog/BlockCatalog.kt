package xtdb.catalog

import org.slf4j.LoggerFactory
import xtdb.BufferPool
import xtdb.api.TransactionKey
import xtdb.api.log.MessageId
import xtdb.block.proto.Block
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.table.DatabaseName
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.BlockIndex
import xtdb.trie.Trie.tablePath
import xtdb.util.StringUtil.asLexHex
import xtdb.util.StringUtil.fromLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.io.path.extension

private val LOGGER = LoggerFactory.getLogger(BlockCatalog::class.java)

class BlockCatalog(private val dbName: DatabaseName, private val bp: BufferPool) {

    companion object {
        private val blocksPath = "blocks".asPath
    }

    private val allBlockFiles get() = bp.listAllObjects(blocksPath).filter { it.key.fileName.extension == "binpb" }

    @Volatile
    private var latestBlock: Block? =
        allBlockFiles.lastOrNull()?.key
            ?.let { blockKey -> Block.parseFrom(bp.getByteArray(blockKey)) }

    fun blockFromLatest(distance: Int): Block? =
        allBlockFiles.toList().dropLast(maxOf(0, distance - 1)).lastOrNull()?.key
            ?.let { blockKey -> Block.parseFrom(bp.getByteArray(blockKey)) }

    fun finishBlock(
        blockIndex: BlockIndex,
        latestCompletedTx: TransactionKey?,
        latestProcessedMsgId: MessageId,
        tables: Collection<TableRef>
    ) {
        val currentBlockIndex = this.currentBlockIndex
        check(currentBlockIndex == null || currentBlockIndex < blockIndex) {
            "Cannot finish block $blockIndex when current block is $currentBlockIndex"
        }

        val newBlock = block {
            this.blockIndex = blockIndex
            latestCompletedTx?.also { tx ->
                this.latestCompletedTx = txKey {
                    txId = tx.txId
                    systemTime = tx.systemTime.asMicros
                }
            }
            this.latestProcessedMsgId = latestProcessedMsgId
            this.tableNames.addAll(tables.map { it.sym.toString() })
        }

        bp.putObject(blocksPath.resolve("b${blockIndex.asLexHex}.binpb"), ByteBuffer.wrap(newBlock.toByteArray()))

        this.latestBlock = newBlock
    }

    val currentBlockIndex get() = latestBlock?.blockIndex

    val latestCompletedTx: TransactionKey?
        get() = latestBlock
            ?.takeIf { it.hasLatestCompletedTx() }
            ?.latestCompletedTx
            ?.let { TransactionKey(it.txId, it.systemTime.microsAsInstant) }

    val latestProcessedMsgId: MessageId?
        get() = latestBlock?.let { block -> block.latestProcessedMsgId.takeIf { block.hasLatestProcessedMsgId() } }
            ?: latestCompletedTx?.txId

    val allTables: List<TableRef> get() = latestBlock?.tableNamesList.orEmpty().map { TableRef.parse(dbName, it) }

    private fun Path.parseBlockIndex(): Long? =
        Regex("b(\\p{XDigit}+)\\.binpb")
            .matchEntire(fileName.toString())
            ?.groups?.get(1)
            ?.value?.fromLexHex

    private fun deleteBlock(blockPath: Path) {
        check(blockPath.parseBlockIndex() != currentBlockIndex) {
            "Cannot delete current block $blockPath - aborting"
        }
        LOGGER.debug("Deleting block file {}", blockPath)
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

        allTables.shuffled().take(100).forEach { table ->
            LOGGER.debug("Garbage collecting blocks for table {}, keeping {} blocks", table.sym, blocksToKeep)
            val tableBlockPath = table.tablePath.resolve(blocksPath)
            deleteOldestBlocks(tableBlockPath, blocksToKeep)
        }
    }
}