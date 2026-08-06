package xtdb.indexer

import xtdb.api.TransactionKey
import xtdb.api.TableRef
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog.Companion.mergeTypeMaps
import xtdb.catalog.TableCatalog.Companion.mergeTypesByTable
import xtdb.trie.ColumnName
import xtdb.util.closeAll
import java.time.Instant

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
        fun openSnapshot(minBasis: List<Instant?>?): DatabaseSnapshot
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

    /**
     * Live type of [column] in [table], merged across [partitions]; null if no partition has it.
     *
     * Via [columnTypes] rather than reducing each partition's `columnType`, so the two can't
     * disagree — a partition that has the table but not the column has to widen the merged type
     * to nullable rather than drop out of it.
     */
    fun columnType(table: TableRef, column: ColumnName): VectorType? = columnTypes(table)?.get(column)

    /** Live `{column → type}` for [table], merged across [partitions]; null if no partition has it. */
    fun columnTypes(table: TableRef): Map<ColumnName, VectorType>? =
        mergeTypeMaps(partitions.map { it.allColumnTypes[table] })

    /** Live `{table → {column → type}}` merged across [partitions]. */
    val allColumnTypes: Map<TableRef, Map<ColumnName, VectorType>>
        get() = mergeTypesByTable(partitions.map { it.allColumnTypes })

    /** One [TransactionKey] per partition, in partition-index order. */
    val txBasis: List<TransactionKey?> get() = partitions.map { it.txBasis }

    override fun close() = partitions.closeAll()
}
