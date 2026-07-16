package xtdb

import xtdb.api.Xtdb
import xtdb.database.Database

interface XtdbInternal : Xtdb {
    val dbCatalog: Database.Catalog
}