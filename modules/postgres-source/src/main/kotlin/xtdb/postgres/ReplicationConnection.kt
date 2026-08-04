package xtdb.postgres

import org.postgresql.PGProperty
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

/**
 * Opens a connection in Postgres' replication mode, on which the replication *protocol* commands
 * (`CREATE_REPLICATION_SLOT`, `START_REPLICATION`, `ALTER_REPLICATION_SLOT`) are accepted. None of
 * them is SQL, and several have no SQL-function equivalent at all, so an ordinary pooled connection
 * cannot reach them.
 *
 * Simple query mode goes with it: a `replication=database` connection rejects the extended query
 * protocol pgjdbc otherwise defaults to.
 *
 * @suppress
 */
// PGProperty.set() is the correct API here — it resolves the internal property name string.
// props[PGProperty.USER] = "..." silently breaks: it puts the enum object as the key,
// but pgjdbc looks up by string (e.g. "user"), so the password is never found.
internal fun openReplicationConnection(
    hostname: String, port: Int, database: String,
    username: String, password: String,
): Connection =
    DriverManager.getConnection(
        "jdbc:postgresql://$hostname:$port/$database",
        Properties().also {
            PGProperty.USER.set(it, username)
            PGProperty.PASSWORD.set(it, password)
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(it, "15")
            PGProperty.REPLICATION.set(it, "database")
            PGProperty.PREFER_QUERY_MODE.set(it, "simple")
        })
