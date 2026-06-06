package xtdb.api

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class PasswordHashTest {

    @Test
    fun `argon2id round-trips`() {
        val hash = PasswordHash.argon2id("hunter2")

        assertTrue(hash.encoded.startsWith("\$argon2id\$"), "encodes as a PHC string")
        assertTrue(hash.verify("hunter2"))
        assertFalse(hash.verify("wrong"))
    }

    @Test
    fun `bcrypt round-trips`() {
        val hash = PasswordHash.bcrypt("hunter2")

        assertTrue(hash.encoded.startsWith("\$2"), "encodes as a modular-crypt bcrypt string")
        assertTrue(hash.verify("hunter2"))
        assertFalse(hash.verify("wrong"))
    }

    @Test
    fun `distinct salts produce distinct encodings for the same password`() {
        assertNotEquals(PasswordHash.argon2id("hunter2").encoded, PasswordHash.argon2id("hunter2").encoded)
        assertNotEquals(PasswordHash.bcrypt("hunter2").encoded, PasswordHash.bcrypt("hunter2").encoded)
    }

    @Test
    fun `a stored encoding keeps verifying — the parse-then-rederive contract holds`() {
        val encoded = PasswordHash.argon2id("correct horse").encoded

        assertTrue(PasswordHash.Argon2id(encoded).verify("correct horse"))
        assertFalse(PasswordHash.Argon2id(encoded).verify("correct horse battery"))
    }

    @Test
    fun `verify returns false rather than throwing on a malformed encoding`() {
        assertFalse(PasswordHash.Argon2id("not-a-real-hash").verify("hunter2"))
        assertFalse(PasswordHash.BCrypt("not-a-real-hash").verify("hunter2"))
    }
}
