package xtdb.expression

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Tests for PostgreSQL-compatible format function.
 * Test cases derived from https://www.postgresql.org/docs/current/functions-string.html
 */
class PgFormatTest {

    @Test
    fun `basic format conversions`() {
        // format('Hello %s', 'World') → Hello World
        assertEquals("Hello World", pgFormat("Hello %s", listOf("World")))

        // format('Testing %s, %s, %s, %%', 'one', 'two', 'three') → Testing one, two, three, %
        assertEquals("Testing one, two, three, %", pgFormat("Testing %s, %s, %s, %%", listOf("one", "two", "three")))
    }

    @Test
    fun `identifier and literal quoting`() {
        // format('INSERT INTO %I VALUES(%L)', 'Foo bar', E'O\'Reilly')
        // → INSERT INTO "Foo bar" VALUES('O''Reilly')
        assertEquals(
            "INSERT INTO \"Foo bar\" VALUES('O''Reilly')",
            pgFormat("INSERT INTO %I VALUES(%L)", listOf("Foo bar", "O'Reilly"))
        )

        // format('INSERT INTO %I VALUES(%L)', 'locations', 'C:\Program Files')
        // → INSERT INTO locations VALUES('C:\Program Files')
        assertEquals(
            "INSERT INTO locations VALUES('C:\\Program Files')",
            pgFormat("INSERT INTO %I VALUES(%L)", listOf("locations", "C:\\Program Files"))
        )
    }

    @Test
    fun `width fields`() {
        // format('|%10s|', 'foo') → |       foo|
        assertEquals("|       foo|", pgFormat("|%10s|", listOf("foo")))

        // format('|%-10s|', 'foo') → |foo       |
        assertEquals("|foo       |", pgFormat("|%-10s|", listOf("foo")))
    }

    @Test
    fun `width from argument`() {
        // format('|%*s|', 10, 'foo') → |       foo|
        assertEquals("|       foo|", pgFormat("|%*s|", listOf("10", "foo")))

        // format('|%*s|', -10, 'foo') → |foo       |
        assertEquals("|foo       |", pgFormat("|%*s|", listOf("-10", "foo")))

        // format('|%-*s|', 10, 'foo') → |foo       |
        assertEquals("|foo       |", pgFormat("|%-*s|", listOf("10", "foo")))

        // format('|%-*s|', -10, 'foo') → |foo       |
        assertEquals("|foo       |", pgFormat("|%-*s|", listOf("-10", "foo")))
    }

    @Test
    fun `position specifiers`() {
        // format('Testing %3$s, %2$s, %1$s', 'one', 'two', 'three') → Testing three, two, one
        assertEquals(
            "Testing three, two, one",
            pgFormat("Testing %3\$s, %2\$s, %1\$s", listOf("one", "two", "three"))
        )
    }

    @Test
    fun `positional width from argument`() {
        // format('|%*2$s|', 'foo', 10, 'bar') → |       bar|
        assertEquals("|       bar|", pgFormat("|%*2\$s|", listOf("foo", "10", "bar")))

        // format('|%1$*2$s|', 'foo', 10, 'bar') → |       foo|
        assertEquals("|       foo|", pgFormat("|%1\$*2\$s|", listOf("foo", "10", "bar")))
    }

    @Test
    fun `mixed positional and sequential`() {
        // format('Testing %3$s, %2$s, %s', 'one', 'two', 'three') → Testing three, two, three
        assertEquals(
            "Testing three, two, three",
            pgFormat("Testing %3\$s, %2\$s, %s", listOf("one", "two", "three"))
        )
    }

    @Test
    fun `null handling for %s`() {
        assertEquals("value: ", pgFormat("value: %s", listOf(null)))
    }

    @Test
    fun `null handling for %L`() {
        assertEquals("value: NULL", pgFormat("value: %L", listOf(null)))
    }

    @Test
    fun `null handling for %I throws`() {
        val ex = assertThrows<IllegalArgumentException> {
            pgFormat("%I", listOf(null))
        }
        assertEquals("null values cannot be formatted as an SQL identifier", ex.message)
    }

    @Test
    fun `identifier quoting rules`() {
        // lowercase simple identifier - no quoting needed
        assertEquals("foo", pgFormat("%I", listOf("foo")))

        // uppercase needs quoting
        assertEquals("\"Foo\"", pgFormat("%I", listOf("Foo")))

        // spaces need quoting
        assertEquals("\"foo bar\"", pgFormat("%I", listOf("foo bar")))

        // starts with number needs quoting
        assertEquals("\"123abc\"", pgFormat("%I", listOf("123abc")))

        // special characters need quoting
        assertEquals("\"foo-bar\"", pgFormat("%I", listOf("foo-bar")))

        // embedded quotes are escaped
        assertEquals("\"foo\"\"bar\"", pgFormat("%I", listOf("foo\"bar")))

        // empty string needs quoting
        assertEquals("\"\"", pgFormat("%I", listOf("")))
    }

    @Test
    fun `literal quoting rules`() {
        // simple string
        assertEquals("'foo'", pgFormat("%L", listOf("foo")))

        // embedded single quotes are escaped
        assertEquals("'O''Reilly'", pgFormat("%L", listOf("O'Reilly")))

        // backslashes are preserved
        assertEquals("'C:\\Program Files'", pgFormat("%L", listOf("C:\\Program Files")))
    }
}
