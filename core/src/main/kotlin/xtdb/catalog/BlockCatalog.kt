package xtdb.catalog

import xtdb.api.TransactionKey
import xtdb.api.log.MessageId
import xtdb.api.storage.ObjectStore
import xtdb.block.proto.Block
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.database.proto.DatabaseConfig
import xtdb.storage.BufferPool
import xtdb.table.DatabaseName
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.BlockIndex
import xtdb.trie.Trie.tablePath
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import java.nio.file.Path
import kotlin.io.path.extension

class BlockCatalog(
    private val dbName: DatabaseName, 
    @field:Volatile private var latestBlock: Block?
) {

    companion object {
        private val blocksPath = "blocks".asPath

        @JvmStatic
        fun blockFilePath(blockIndex: BlockIndex): Path =
            blocksPath.resolve("b${blockIndex.asLexHex}.binpb")

        @JvmStatic
        fun tableBlockPath(table: TableRef, blockIndex: BlockIndex): Path =
            table.tablePath.resolve(blocksPath).resolve("b${blockIndex.asLexHex}.binpb")

        val BufferPool.allBlockFiles: Iterable<ObjectStore.StoredObject>
            get() = listAllObjects(blocksPath).filter { it.key.fileName.extension == "binpb" }

        fun BufferPool.tableBlocks(table: TableRef): Iterable<ObjectStore.StoredObject> =
            listAllObjects(table.tablePath.resolve(blocksPath))

        @JvmStatic
        val BufferPool.latestBlock: Block?
            get() = allBlockFiles.lastOrNull()?.key
                ?.let { blockKey -> Block.parseFrom(getByteArray(blockKey)) }

        fun BufferPool.blockFromLatest(distance: Int): Block? =
            allBlockFiles.toList().dropLast(maxOf(0, distance - 1)).lastOrNull()?.key
                ?.let { blockKey -> Block.parseFrom(getByteArray(blockKey)) }
    }

    fun refresh(block: Block?) {
        if (block != null && block.blockIndex == currentBlockIndex) return
        latestBlock = block
    }

    fun buildBlock(
        blockIndex: BlockIndex,
        latestCompletedTx: TransactionKey?,
        latestProcessedMsgId: MessageId,
        tables: Collection<TableRef>,
        secondaryDatabases: Map<String, DatabaseConfig>?
    ): Block {
        val currentBlockIndex = this.currentBlockIndex
        check(currentBlockIndex == null || currentBlockIndex < blockIndex) {
            "Cannot finish block $blockIndex when current block is $currentBlockIndex"
        }

        return block {
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
