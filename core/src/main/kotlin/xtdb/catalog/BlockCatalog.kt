package xtdb.catalog

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import xtdb.api.log.LeaderTerm
import xtdb.api.tx.BlockDetails
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.TransactionKey
import xtdb.types.MessageId
import xtdb.api.storage.ObjectStore
import xtdb.block.proto.Block
import xtdb.block.proto.block
import xtdb.block.proto.txKey
import xtdb.database.proto.DatabaseConfig
import xtdb.storage.BufferPool
import xtdb.api.TableRef
import xtdb.table.fromSchemaAndTable
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.BlockIndex
import xtdb.trie.Trie.tablePath
import xtdb.util.StringUtil.asLexHex
import xtdb.util.asPath
import java.nio.file.Path
import kotlin.io.path.extension

// The proto is the storage format, so this is the only place that reads its presence flags: everything
// downstream sees ordinary Kotlin nullability.
private fun Block.asBlockDetails() = BlockDetails(
    blockIndex = blockIndex,
    latestCompletedTx = takeIf { it.hasLatestCompletedTx() }?.latestCompletedTx
        ?.let { TransactionKey(it.txId, it.systemTime.microsAsInstant) },
    latestProcessedMsgId = latestProcessedMsgId.takeIf { hasLatestProcessedMsgId() },
    boundaryReplicaMsgId = boundaryReplicaMsgId.takeIf { hasBoundaryReplicaMsgId() },
    termId = termId,
    externalSourceToken = takeIf { it.hasExternalSourceToken() }?.externalSourceToken?.toByteArray(),
    tableNames = tableNamesList.map { fromSchemaAndTable(it) },
    secondaryDatabases = secondaryDatabasesMap,
)

class BlockCatalog(initialBlock: Block?) {

    private val _latestBlock = MutableStateFlow(initialBlock?.asBlockDetails())

    /**
     * The latest block this catalog knows to be in object storage, advancing on [refresh] — which the
     * leader calls once the block file has landed, and a follower once it has read the block back.
     *
     * A collector is therefore observing durability, not resolution: anything this emits is recoverable
     * from storage alone. That is what lets an external source use the emitted `externalSourceToken` as
     * the furthest position it may confirm upstream.
     */
    val latestBlock: StateFlow<BlockDetails?> = _latestBlock.asStateFlow()

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
        _latestBlock.value = block?.asBlockDetails()
    }

    fun buildBlock(
        blockIndex: BlockIndex,
        latestCompletedTx: TransactionKey?,
        latestProcessedMsgId: MessageId,
        boundaryReplicaMsgId: MessageId?,
        tables: Collection<TableRef>,
        secondaryDatabases: Map<String, DatabaseConfig>?,
        externalSourceToken: ExternalSourceToken? = null,
        termId: Long = LeaderTerm.NONE,
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
            boundaryReplicaMsgId?.let { this.boundaryReplicaMsgId = it }
            this.tableNames.addAll(tables.map { it.sym.toString() })
            secondaryDatabases?.let { this.secondaryDatabases.putAll(it) }
            externalSourceToken?.let { this.externalSourceToken = ByteString.copyFrom(it) }
            this.termId = termId
        }
    }

    val currentBlockIndex: BlockIndex? get() = _latestBlock.value?.blockIndex

    val latestCompletedTx: TransactionKey? get() = _latestBlock.value?.latestCompletedTx

    val latestProcessedMsgId: MessageId?
        get() = _latestBlock.value?.latestProcessedMsgId ?: latestCompletedTx?.txId

    val boundaryReplicaMsgId: MessageId? get() = _latestBlock.value?.boundaryReplicaMsgId

    // the leader term that produced the latest block's boundary; a follower seeds its read-side term
    // fence from here. Default 0 (plain scalar) for blocks written before term-fencing. See #5817.
    val boundaryTermId: Long get() = _latestBlock.value?.termId ?: LeaderTerm.NONE

    val externalSourceToken: ExternalSourceToken? get() = _latestBlock.value?.externalSourceToken

    val allTables: List<TableRef> get() = _latestBlock.value?.tableNames.orEmpty()

    val secondaryDatabases: Map<String, DatabaseConfig> get() = _latestBlock.value?.secondaryDatabases.orEmpty()
}
