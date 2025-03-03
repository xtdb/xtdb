package xtdb.catalog

import xtdb.BufferPool
import xtdb.api.TransactionKey
import xtdb.block.proto.Block
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.BlockIndex
import xtdb.trie.TableName
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer

class BlockCatalog(private val bp: BufferPool) {

    @Volatile
    private var latestBlock: Block? =
        bp.listAllObjects(blocksPath).lastOrNull()?.key
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
}