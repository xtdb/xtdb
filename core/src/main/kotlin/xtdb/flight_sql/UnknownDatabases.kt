package xtdb.flight_sql

import xtdb.api.Xtdb
import xtdb.database.Database
import xtdb.database.DatabaseName

/** What the Flight SQL server does with a call naming a database this node hasn't attached. */
sealed interface UnknownDatabases {

    /** Whether a session may select [dbName] as its `catalog`, attaching it first where this policy attaches. */
    fun admitCatalog(dbName: DatabaseName): Boolean

    /** Runs before a session opens a connection to [dbName]. */
    fun beforeConnect(dbName: DatabaseName)

    /** A session can't select it as its `catalog`, and statements on a connection to it meet an unknown database. */
    class Reject(private val node: Xtdb) : UnknownDatabases {
        override fun admitCatalog(dbName: DatabaseName) = dbName in node.databaseNames
        override fun beforeConnect(dbName: DatabaseName) = Unit
    }

    /** The first call naming it attaches it to this node alone, with the default config — as the playground does. */
    class AttachOnFirstUse(private val dbCatalog: Database.Catalog) : UnknownDatabases {
        override fun admitCatalog(dbName: DatabaseName) = dbCatalog.databaseOrAttach(dbName) != null

        override fun beforeConnect(dbName: DatabaseName) {
            dbCatalog.databaseOrAttach(dbName)
        }
    }
}
