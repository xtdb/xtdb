package xtdb.mysql

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.api.error.Incorrect
import xtdb.api.nodeConfig
import xtdb.database.Database

class MySqlSourceFactoryTest {

    private fun protoRoundTrip(factory: MySqlSource.Factory): MySqlSource.Factory {
        val dbConfig = Database.Config().externalSource(factory)
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)
        return restored.externalSource as MySqlSource.Factory
    }

    @Test
    fun `proto round-trips factory`() {
        val original = MySqlSource.Factory(
            remote = "mysql", serverId = 5701, tables = listOf("shop.users", "shop.orders"),
        )

        assertEquals(original, protoRoundTrip(original))
    }

    @Test
    fun `YAML round-trips factory as external source`() {
        val yaml = """
            externalSource: !MySQL
              remote: mysql
              serverId: 5701
              tables: [shop.users, shop.orders]
        """.trimIndent()

        val factory = Database.Config.fromYaml(yaml).externalSource as MySqlSource.Factory

        assertEquals("mysql", factory.remote)
        assertEquals(5701L, factory.serverId)
        assertEquals(listOf("shop.users", "shop.orders"), factory.tables)
    }

    @Test
    fun `node config decodes MySQL remote under remotes`() {
        val yaml = """
            remotes:
              mysql: !MySQL
                hostname: mysql.prod.internal
                port: 3307
                username: xtdb
                password: secret
        """.trimIndent()

        val remote = nodeConfig(yaml).remotes["mysql"] as MySqlRemote.Factory

        assertEquals("mysql.prod.internal", remote.hostname)
        assertEquals(3307, remote.port)
        assertEquals("xtdb", remote.username)
        assertEquals("secret", remote.password)
    }

    @Test
    fun `port defaults to 3306 when omitted`() {
        val yaml = """
            remotes:
              mysql: !MySQL
                hostname: localhost
                username: u
                password: p
        """.trimIndent()

        assertEquals(3306, (nodeConfig(yaml).remotes["mysql"] as MySqlRemote.Factory).port)
    }

    @Test
    fun `dual !MySQL tag is disambiguated by YAML position`() {
        val nodeRemote = nodeConfig(
            """
            remotes:
              mysql: !MySQL
                hostname: localhost
                username: u
                password: p
            """.trimIndent()
        ).remotes["mysql"]

        val extSource = Database.Config.fromYaml(
            """
            externalSource: !MySQL
              remote: mysql
              serverId: 1234
              tables: [a.b]
            """.trimIndent()
        ).externalSource

        assertTrue(nodeRemote is MySqlRemote.Factory)
        assertTrue(extSource is MySqlSource.Factory)
    }

    private fun open(factory: MySqlSource.Factory, remotes: Map<String, xtdb.api.Remote> = mapOf("mysql" to MySqlRemote("h", 3306, "u", "p"))) =
        factory.open("db", remotes, null)

    @Test
    fun `a server id outside the 32-bit range is rejected on open`() {
        for (id in listOf(0L, -1L, 4_294_967_296L)) {
            val e = assertThrows(Incorrect::class.java) {
                open(MySqlSource.Factory("mysql", id, listOf("a.b")))
            }
            assertEquals("xtdb.mysql/invalid-server-id", e.errorCode())
        }
    }

    @Test
    fun `an empty include-list and a table that isn't database dot table are rejected on open`() {
        assertEquals("xtdb.mysql/no-tables", assertThrows(Incorrect::class.java) { open(MySqlSource.Factory("mysql", 1, emptyList())) }.errorCode())
        assertEquals("xtdb.mysql/invalid-table-name", assertThrows(Incorrect::class.java) { open(MySqlSource.Factory("mysql", 1, listOf("just_a_table"))) }.errorCode())
    }

    @Test
    fun `an unknown remote alias is rejected on open`() {
        assertEquals("xtdb.mysql/missing-remote", assertThrows(Incorrect::class.java) { open(MySqlSource.Factory("nope", 1, listOf("a.b"))) }.errorCode())
    }
}
