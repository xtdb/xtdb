package xtdb.postgres

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.nodeConfig
import xtdb.database.Database

class PostgresSourceFactoryTest {

    private fun protoRoundTrip(factory: PostgresSource.Factory): PostgresSource.Factory {
        val dbConfig = Database.Config().externalSource(factory)
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)
        return restored.externalSource as PostgresSource.Factory
    }

    @Test
    fun `proto round-trips factory`() {
        val original = PostgresSource.Factory(
            remote = "pg",
            slotName = "my_slot",
            publicationName = "my_pub",
            schemaIncludeList = listOf("public", "analytics"),
        )

        val restored = protoRoundTrip(original)

        assertEquals("pg", restored.remote)
        assertEquals("my_slot", restored.slotName)
        assertEquals("my_pub", restored.publicationName)
        assertEquals(listOf("public", "analytics"), restored.schemaIncludeList)
    }

    @Test
    fun `YAML round-trips factory as external source`() {
        val yaml = """
            externalSource: !Postgres
              remote: pg
              slotName: my_slot
              publicationName: my_pub
              schemaIncludeList: [public]
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val factory = config.externalSource as PostgresSource.Factory

        assertEquals("pg", factory.remote)
        assertEquals("my_slot", factory.slotName)
        assertEquals("my_pub", factory.publicationName)
        assertEquals(listOf("public"), factory.schemaIncludeList)
    }

    @Test
    fun `node config decodes Postgres remote under remotes`() {
        val yaml = """
            remotes:
              pg: !Postgres
                hostname: pg.prod.internal
                port: 5433
                database: app
                username: xtdb
                password: secret
        """.trimIndent()

        val config = nodeConfig(yaml)
        val remote = config.remotes["pg"] as PostgresRemote.Factory

        assertEquals("pg.prod.internal", remote.hostname)
        assertEquals(5433, remote.port)
        assertEquals("app", remote.database)
        assertEquals("xtdb", remote.username)
        assertEquals("secret", remote.password)
    }

    @Test
    fun `port defaults to 5432 when omitted`() {
        val yaml = """
            remotes:
              pg: !Postgres
                hostname: localhost
                database: test
                username: u
                password: p
        """.trimIndent()

        val remote = nodeConfig(yaml).remotes["pg"] as PostgresRemote.Factory

        assertEquals(5432, remote.port)
    }

    @Test
    fun `dual !Postgres tag is disambiguated by YAML position`() {
        // Both PostgresSource.Factory (ExternalSource) and PostgresRemote.Factory (Remote)
        // carry @SerialName("!Postgres"). Kaml must dispatch by polymorphic root, not by tag.
        val nodeYaml = """
            remotes:
              pg: !Postgres
                hostname: localhost
                database: db
                username: u
                password: p
        """.trimIndent()

        val dbYaml = """
            externalSource: !Postgres
              remote: pg
              slotName: s
              publicationName: p
        """.trimIndent()

        val nodeRemote = nodeConfig(nodeYaml).remotes["pg"]
        val extSource = Database.Config.fromYaml(dbYaml).externalSource

        assertTrue(nodeRemote is PostgresRemote.Factory, "node remote should resolve to PostgresRemote.Factory")
        assertTrue(extSource is PostgresSource.Factory, "external source should resolve to PostgresSource.Factory")
    }
}
