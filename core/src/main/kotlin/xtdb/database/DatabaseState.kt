package xtdb.database

import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.trie.TrieCatalog

data class DatabaseState(
    val name: DatabaseName,
    val blockCatalogOrNull: BlockCatalog?,
    val tableCatalogOrNull: TableCatalog?,
    val trieCatalogOrNull: TrieCatalog?,
) {
    val blockCatalog: BlockCatalog get() = blockCatalogOrNull ?: error("no block-catalog")
    val tableCatalog: TableCatalog get() = tableCatalogOrNull ?: error("no table-catalog")
    val trieCatalog: TrieCatalog get() = trieCatalogOrNull ?: error("no trie-catalog")
}
