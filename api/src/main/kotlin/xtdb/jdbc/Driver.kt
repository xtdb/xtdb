package xtdb.jdbc

import org.postgresql.jdbc.PgConnection
import java.sql.Connection
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

    override fun connect(url: String, info: Properties?): Connection? {
        val props = (info ?: Properties()).apply {
            putIfAbsent("binaryTransferDisable", "1184")  // Disable binary for timestamptz
        }

        return (super.connect(url.asPgUrl, props) as? PgConnection)?.let(::XtConnection)
            ?.also { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("SET fallback_output_format = 'transit'")
                    stmt.execute("SET datestyle = 'iso8601'")
                }
            }
    }
}
