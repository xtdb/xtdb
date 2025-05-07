package xtdb.api

import java.sql.Connection

interface DataSource : javax.sql.DataSource {
    override fun getConnection() = createConnectionBuilder().build()

    override fun getConnection(username: String?, password: String?) =
        createConnectionBuilder().user(username).password(password).build()

    interface ConnectionBuilder : java.sql.ConnectionBuilder {
        fun port(port: Int): ConnectionBuilder
        override fun user(username: String?): ConnectionBuilder
        override fun password(password: String?): ConnectionBuilder
        fun database(password: String?): ConnectionBuilder
        fun option(key: String, value: Any?): ConnectionBuilder
        override fun build(): Connection
    }

    override fun createConnectionBuilder(): ConnectionBuilder
}