package xtdb

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import xtdb.api.Authenticator
import xtdb.api.Authenticator.Factory.UserList
import xtdb.api.Authenticator.Method.PASSWORD
import xtdb.api.Authenticator.MethodRule
import xtdb.api.PasswordHash
import xtdb.api.Xtdb
import xtdb.api.error.Forbidden
import xtdb.api.error.Incorrect
import xtdb.api.error.Unsupported

class NodeAuthnTest {

    private var node: Xtdb? = null

    private fun openNode(authn: Authenticator.Factory): Xtdb =
        Xtdb.openNode { authn(authn) }.also { node = it }

    @AfterEach
    fun tearDown() {
        node?.close()
    }

    private val users = mapOf(
        "xtdb" to PasswordHash.argon2id("root-pw"),
        "alice" to PasswordHash.argon2id("alice-pw"),
    )

    private fun Xtdb.Connection.executeUpdate(sql: String) {
        createStatement().use { stmt ->
            stmt.setSqlQuery(sql)
            stmt.executeUpdate()
        }
    }

    @Test
    fun `a password connection runs as the user it authenticated as`() {
        val node = openNode(UserList(users))

        assertEquals(PASSWORD, node.authMethodFor("alice", null))

        node.connectWithPassword("xtdb", "alice", "alice-pw").use { conn ->
            assertEquals("alice", conn.user)
        }
    }

    @Test
    fun `a wrong password opens no connection`() {
        val node = openNode(UserList(users))

        assertEquals(
            "password authentication failed for user: alice",
            assertThrows<Forbidden> { node.connectWithPassword("xtdb", "alice", "wrong") }.message
        )
    }

    @Test
    fun `only a connection authenticated as the superuser may grant role membership`() {
        val node = openNode(UserList(users))

        node.connectWithPassword("xtdb", "xtdb", "root-pw").use { conn ->
            assertDoesNotThrow { conn.executeUpdate("GRANT analyst TO alice") }
        }

        node.connectWithPassword("xtdb", "alice", "alice-pw").use { conn ->
            assertEquals(
                "Only a superuser may GRANT/REVOKE role membership.",
                assertThrows<Incorrect> { conn.executeUpdate("GRANT analyst TO bob") }.message
            )
        }

        node.connect().use { conn ->
            assertEquals(
                "Only a superuser may GRANT/REVOKE role membership.",
                assertThrows<Incorrect> { conn.executeUpdate("GRANT analyst TO bob") }.message
            )
        }
    }

    @Test
    fun `a trusted connection runs as the user it names, whatever the rules ask`() {
        val node = openNode(UserList(users))

        assertEquals(PASSWORD, node.authMethodFor("alice", null))

        node.connectTrusted("xtdb", "alice").use { conn ->
            assertEquals("alice", conn.user)
        }

        node.connectTrusted("xtdb", null).use { conn ->
            assertNull(conn.user)
        }
    }

    @Test
    fun `a caller no rule matches is told so`() {
        val node = openNode(UserList(users, listOf(MethodRule(PASSWORD, user = "alice"))))

        assertNull(node.authMethodFor("mallory", null))
    }

    @Test
    fun `an authenticator without OAuth support refuses the OAuth flows`() {
        val node = openNode(UserList(users))

        assertThrows<Unsupported> { node.connectWithClientCredentials("xtdb", "client", "secret") }
        assertThrows<Unsupported> { node.startDeviceAuth("xtdb", "alice") }
    }
}
