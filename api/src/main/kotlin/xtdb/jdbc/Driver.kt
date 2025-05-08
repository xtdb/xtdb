package xtdb.jdbc

import org.postgresql.jdbc.PgConnection
import java.sql.*
import java.util.*

class Driver : org.postgresql.Driver() {

    companion object {
        init {
            DriverManager.registerDriver(Driver())
        }
    }

    private val String.asPgUrl get() = replace(Regex("^jdbc:xtdb:"), "jdbc:postgresql:")

    override fun acceptsURL(url: String) = super.acceptsURL(url.asPgUrl)

    override fun connect(url: String, info: Properties?): Connection? =
        (super.connect(url.asPgUrl, info) as? PgConnection)?.let(::XtConnection)
            ?.also { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("SET FALLBACK_OUTPUT_FORMAT = 'transit'")
                }
            }
}