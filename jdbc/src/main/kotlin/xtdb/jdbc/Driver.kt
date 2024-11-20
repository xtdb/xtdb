package xtdb.jdbc

import org.postgresql.PGConnection
import org.postgresql.jdbc.PgConnection
import java.sql.DriverManager
import java.util.*

class Driver : org.postgresql.Driver() {

    companion object {
        init {
            DriverManager.registerDriver(Driver())
        }
    }

    private val String.asPgUrl get() = replace(Regex("^jdbc:xtdb:"), "jdbc:postgresql:")

    override fun acceptsURL(url: String) = super.acceptsURL(url.asPgUrl)

    class Connection(conn: PgConnection) : PGConnection by conn, java.sql.Connection by conn

    override fun connect(url: String, info: Properties?) =
        (super.connect(url.asPgUrl, info) as? PgConnection)?.let(::Connection)
}