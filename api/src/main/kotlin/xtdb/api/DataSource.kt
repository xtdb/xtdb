package xtdb.api

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLFeatureNotSupportedException
import java.sql.ShardingKey
import java.util.*

interface DataSource : javax.sql.DataSource {
    var awaitToken: String

    override fun getConnection() = createConnectionBuilder().build()

    override fun getConnection(username: String?, password: String?) =
        createConnectionBuilder().user(username).password(password).build()

    class ConnectionBuilder
    @JvmOverloads constructor(
        private val host: String, private var port: Int = 5432,
        private var user: String? = null, private var password: String? = null,
        private var database: String = "xtdb", opts: Map<String, Any?> = emptyMap()
    ) : java.sql.ConnectionBuilder {
        private val opts = opts.toMutableMap()

        fun port(port: Int) = apply { this.port = port }

        override fun user(username: String?) = apply { this.user = username }
        override fun password(password: String?) = apply { this.password = password }

        override fun shardingKey(shardingKey: ShardingKey?) =
            throw SQLFeatureNotSupportedException("shardingKey")

        override fun superShardingKey(superShardingKey: ShardingKey?) =
            throw SQLFeatureNotSupportedException("superShardingKey")

        fun database(database: String) = apply { this.database = database }
        fun option(key: String, value: Any?) = apply { opts[key] = value }

        override fun build(): Connection =
            DriverManager.getConnection(
                "jdbc:xtdb://$host:$port/$database",
                Properties().also { props ->
                    user?.let { props["user"] = it }
                    password?.let { props["password"] = it }
                    opts.forEach { (k, v) -> props[k] = v.toString() }
                }
            )
    }

    override fun createConnectionBuilder(): ConnectionBuilder
}