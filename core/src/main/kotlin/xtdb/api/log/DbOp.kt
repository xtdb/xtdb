package xtdb.api.log

import xtdb.database.Database
import xtdb.database.DatabaseName

/** A change to the node's set of databases: applied by the primary only, since only the primary holds that set. */
sealed interface DbOp : Ddl {
    val dbName: DatabaseName

    data class Attach(override val dbName: DatabaseName, val config: Database.Config) : DbOp
    data class Detach(override val dbName: DatabaseName) : DbOp
}
