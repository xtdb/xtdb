package xtdb.query

import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.operator.scan.ScanEmitter

interface Database {
    val blockCatalog: BlockCatalog
    val tableCatalog: TableCatalog
    val scanEmitter: ScanEmitter
}