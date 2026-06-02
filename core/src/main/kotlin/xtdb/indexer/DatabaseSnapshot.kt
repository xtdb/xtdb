package xtdb.indexer

import xtdb.api.TransactionKey
import xtdb.table.TableRef
import xtdb.trie.ColumnName
import xtdb.util.closeAll

/**
 * Database-level snapshot composed of one [Snapshot] per [xtdb.database.DatabasePartition].
 *
 * Per the stage-4 multi-partition design (issue #5557): cross-partition concepts like
 * `tableInfo` and `txBasis` are unions / per-partition vectors over [partitions]; per-partition
 * reads stay on the underlying [Snapshot] for that partition.
 *
 * For `partitions.size == 1` this is a thin wrapper — every method reduces to the single
 * partition's behaviour.
 */
class DatabaseSnapshot(val partitions: List<Snapshot>) : AutoCloseable {

    interface Source {
        fun openSnapshot(): DatabaseSnapshot
    }

    init {
        require(partitions.isNotEmpty()) { "DatabaseSnapshot must have at least one partition" }
    }

    /** Union of `{table → columns}` across [partitions]. */
    fun tableInfo(): Map<TableRef, Set<ColumnName>> =
        if (partitions.size == 1) partitions[0].tableInfo
        else partitions.fold(emptyMap()) { acc, snap ->
            snap.tableInfo.entries.fold(acc) { a, (table, cols) ->
                a + (table to (a[table].orEmpty() + cols))
            }
        }

    /** One [TransactionKey] per partition, in partition-index order. */
    val txBasis: List<TransactionKey?> get() = partitions.map { it.txBasis }

    override fun close() = partitions.closeAll()
}
