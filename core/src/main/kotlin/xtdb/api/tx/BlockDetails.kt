package xtdb.api.tx

import xtdb.api.TransactionKey
import xtdb.database.proto.DatabaseConfig
import xtdb.table.TableEntry
import xtdb.trie.BlockIndex
import xtdb.types.MessageId

/**
 * What a block persisted to object storage carries.
 *
 * Deliberately knows nothing about the wire format: the block protobuf is the storage format, and the
 * mapping from it lives with the catalog that reads it. Here a field a block may not carry is ordinary
 * nullability, rather than a presence check at each read.
 */
data class BlockDetails(
    val blockIndex: BlockIndex,
    val latestCompletedTx: TransactionKey?,
    val latestProcessedMsgId: MessageId?,
    val boundaryReplicaMsgId: MessageId?,
    // the leader term that produced this block's boundary; NONE for blocks written before
    // term-fencing (see #5817)
    val termId: Long,
    val externalSourceToken: ExternalSourceToken?,
    /** The tables this block records — so, the tables with a per-table block file beside it. */
    val tables: List<TableEntry>,
    val secondaryDatabases: Map<String, DatabaseConfig>,
)
