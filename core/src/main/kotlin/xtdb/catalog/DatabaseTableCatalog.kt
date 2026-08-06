package xtdb.catalog

import xtdb.api.TableRef
import xtdb.arrow.VectorType
import xtdb.catalog.TableCatalog.Companion.mergeTypeMaps
import xtdb.catalog.TableCatalog.Companion.mergeTypesByTable
import xtdb.trie.ColumnName

/**
 * Read-only database-level view of the historical table metadata, merging each partition's own
 * [TableCatalog].
 *
 * Read-only is the point: writes go through the owning partition's indexer, which is the only
 * writer to that partition's catalog, so there is no database-level write to express. `null`
 * means "no partition has this table", not "empty".
 *
 * The type merge is associative and commutative, so the reduction is well-defined whatever order
 * the partitions arrive in.
 */
class DatabaseTableCatalog(private val partitions: List<TableCatalog>) {

    // via [getTypes] rather than reducing each partition's `getType`, so the two can't disagree.
    // Reducing `getType` would drop a partition that has the table but not the column, where the
    // merge has to see that partition's absent column as `Null` and make the merged type nullable.
    fun getType(table: TableRef, columnName: ColumnName): VectorType? = getTypes(table)?.get(columnName)

    fun getTypes(table: TableRef): Map<ColumnName, VectorType>? =
        mergeTypeMaps(partitions.map { it.getTypes(table) })

    fun rowCount(table: TableRef): Long? =
        partitions.mapNotNull { it.rowCount(table) }.reduceOrNull(Long::plus)

    val types: Map<TableRef, Map<ColumnName, VectorType>>
        get() = mergeTypesByTable(partitions.map { it.types })
}
