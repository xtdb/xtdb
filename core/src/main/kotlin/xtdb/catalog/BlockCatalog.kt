package xtdb.catalog

import xtdb.storage.BufferPool
import xtdb.api.TransactionKey
import xtdb.api.log.MessageId
import xtdb.api.storage.ObjectStore
import xtdb.block.proto.Block
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.database.proto.DatabaseConfig
import xtdb.table.DatabaseName
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.BlockIndex
import xtdb.trie.Trie.tablePath
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.io.path.extension

class BlockCatalog(private val dbName: DatabaseName, private val bp: BufferPool) {

    companion object {
        private val blocksPath = "blocks".asPath

        @JvmStatic
        fun blockFilePath(blockIndex: BlockIndex): Path =
            blocksPath.resolve("b${blockIndex.asLexHex}.binpb")
    }

    val allBlockFiles get() = bp.listAllObjects(blocksPath).filter { it.key.fileName.extension == "binpb" }
    fun tableBlocks(table: TableRef) = bp.listAllObjects(table.tablePath.resolve(blocksPath))

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
        tables: Collection<TableRef>,
        secondaryDatabases: Map<String, DatabaseConfig>?
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
            secondaryDatabases?.let { this.secondaryDatabases.putAll(it) }
        }

        bp.putObject(blockFilePath(blockIndex), ByteBuffer.wrap(newBlock.toByteArray()))

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

    val secondaryDatabases: Map<String, DatabaseConfig> get() = latestBlock?.secondaryDatabasesMap.orEmpty()
}