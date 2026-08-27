package xtdb.api.log

import xtdb.database.Database
import xtdb.database.DatabaseName

sealed interface DbOp {
    val dbName: DatabaseName

    data class Attach(override val dbName: DatabaseName, val config: Database.Config) : DbOp
    data class Detach(override val dbName: DatabaseName) : DbOp
}
