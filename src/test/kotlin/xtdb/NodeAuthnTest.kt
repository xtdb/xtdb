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
    fun `connections opened as a password-authenticated principal run as its user`() {
        val node = openNode(UserList(users))

        assertEquals(PASSWORD, node.authMethodFor("alice", null))

        val alice = node.authenticateWithPassword("alice", "alice-pw")
        assertEquals("alice", alice.userId)

        node.connect("xtdb", alice).use { conn -> assertEquals("alice", conn.user) }
        node.connect("xtdb", alice).use { conn -> assertEquals("alice", conn.user) }
    }

    @Test
    fun `a wrong password authenticates no one`() {
        val node = openNode(UserList(users))

        assertEquals(
            "password authentication failed for user: alice",
            assertThrows<Forbidden> { node.authenticateWithPassword("alice", "wrong") }.message
        )
    }

    @Test
    fun `only a connection authenticated as the superuser may grant role membership`() {
        val node = openNode(UserList(users))

        node.connect("xtdb", node.authenticateWithPassword("xtdb", "root-pw")).use { conn ->
            assertDoesNotThrow { conn.executeUpdate("GRANT analyst TO alice") }
        }

        node.connect("xtdb", node.authenticateWithPassword("alice", "alice-pw")).use { conn ->
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
    fun `a trusted principal is the user it names, whatever the rules ask`() {
        val node = openNode(UserList(users))

        assertEquals(PASSWORD, node.authMethodFor("alice", null))

        node.connect("xtdb", node.authenticateTrusted("alice")).use { conn ->
            assertEquals("alice", conn.user)
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

        assertThrows<Unsupported> { node.authenticateWithClientCredentials("client", "secret") }
        assertThrows<Unsupported> { node.startDeviceAuth("alice") }
    }
}
