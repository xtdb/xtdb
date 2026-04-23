package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Authenticator.Method.PASSWORD
import xtdb.api.Authenticator.Method.TRUST
import xtdb.error.Incorrect

class SingleRootUserAuthenticatorTest {

    @Test
    fun `methodFor returns TRUST when no password configured`() {
        val authn = SingleRootUserAuthenticator(null)

        assertEquals(TRUST, authn.methodFor("xtdb", "127.0.0.1"))
        assertEquals(TRUST, authn.methodFor("someone-else", "10.0.0.1"))
    }

    @Test
    fun `methodFor returns PASSWORD when configured`() {
        val authn = SingleRootUserAuthenticator("hunter2")

        assertEquals(PASSWORD, authn.methodFor("xtdb", "127.0.0.1"))
        assertEquals(PASSWORD, authn.methodFor("anyone", "10.0.0.1"))
    }

    @Test
    fun `verifyPassword accepts the root user with the correct password`() {
        val authn = SingleRootUserAuthenticator("hunter2")

        assertEquals(SimpleResult("xtdb"), authn.verifyPassword("xtdb", "hunter2"))
    }

    @Test
    fun `verifyPassword rejects the root user with the wrong password`() {
        val authn = SingleRootUserAuthenticator("hunter2")

        val thrown = assertThrows<Incorrect> { authn.verifyPassword("xtdb", "wrong") }
        assertEquals("password authentication failed for user: xtdb", thrown.message)
    }

    @Test
    fun `verifyPassword rejects any non-root user`() {
        val authn = SingleRootUserAuthenticator("hunter2")

        val thrown = assertThrows<Incorrect> { authn.verifyPassword("alice", "hunter2") }
        assertEquals("password authentication failed for user: alice", thrown.message)
    }

    @Test
    fun `verifyPassword fails when no password was configured`() {
        // methodFor would have returned TRUST here, so this path is only reached
        // if a caller explicitly invokes password verification — still must fail.
        val authn = SingleRootUserAuthenticator(null)

        assertThrows<Incorrect> { authn.verifyPassword("xtdb", "anything") }
    }
}
