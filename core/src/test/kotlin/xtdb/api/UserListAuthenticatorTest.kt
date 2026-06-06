package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Authenticator.Factory.UserList
import xtdb.api.Authenticator.Method.PASSWORD
import xtdb.api.Authenticator.Method.TRUST
import xtdb.api.Authenticator.MethodRule
import xtdb.error.Incorrect

class UserListAuthenticatorTest {

    private val users = mapOf(
        "alice" to PasswordHash.argon2id("alice-pw"),
        "bob" to PasswordHash.bcrypt("bob-pw"),
    )

    @Test
    fun `verifyPassword accepts configured users with the right password`() {
        val authn = UserListAuthenticator(users, listOf(MethodRule(PASSWORD)))

        assertEquals(SimpleResult("alice"), authn.verifyPassword("alice", "alice-pw"))
        assertEquals(SimpleResult("bob"), authn.verifyPassword("bob", "bob-pw"))
    }

    @Test
    fun `verifyPassword rejects a wrong password and an unknown user identically`() {
        val authn = UserListAuthenticator(users, listOf(MethodRule(PASSWORD)))

        assertEquals(
            "password authentication failed for user: alice",
            assertThrows<Incorrect> { authn.verifyPassword("alice", "wrong") }.message
        )
        assertEquals(
            "password authentication failed for user: carol",
            assertThrows<Incorrect> { authn.verifyPassword("carol", "whatever") }.message
        )
    }

    @Test
    fun `methodFor matches rules in order — including remoteAddress`() {
        val authn = UserListAuthenticator(
            users,
            listOf(MethodRule(TRUST, remoteAddress = "127.0.0.1"), MethodRule(PASSWORD)),
        )

        assertEquals(TRUST, authn.methodFor("alice", "127.0.0.1"))
        assertEquals(PASSWORD, authn.methodFor("alice", "10.0.0.1"))
    }

    @Test
    fun `methodFor returns null when no rule matches`() {
        val authn = UserListAuthenticator(users, listOf(MethodRule(PASSWORD, user = "alice")))

        assertEquals(PASSWORD, authn.methodFor("alice", "127.0.0.1"))
        assertNull(authn.methodFor("bob", "127.0.0.1"))
    }

    @Test
    fun `knownUsers lists the configured users`() {
        assertEquals(setOf("alice", "bob"), UserList(users).knownUsers().toSet())
    }
}
