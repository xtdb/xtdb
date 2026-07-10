package xtdb.table

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import xtdb.error.Incorrect

class TableRefTest {

    @Test
    fun `bare name goes in the default schema`() {
        assertEquals(TableRef(DEFAULT_SCHEMA, "foo"), TableRef.parse("foo"))
    }

    @Test
    fun `slash separates schema from table`() {
        assertEquals(TableRef("myschema", "foo"), TableRef.parse("myschema/foo"))
    }

    @Test
    fun `dot separates schema from table`() {
        assertEquals(TableRef("information_schema", "columns"), TableRef.parse("information_schema.columns"))
    }

    @Test
    fun `schema may contain dots`() {
        assertEquals(TableRef("foo.bar", "the-docs"), TableRef.parse("foo.bar/the-docs"))
        assertEquals(TableRef("a.b", "c"), TableRef.parse("a.b.c"))
        assertEquals(TableRef("a.b", "c"), TableRef.parse("a.b/c"))
    }

    @Test
    fun `rejects embedded separators and empty segments`() {
        for (str in listOf("", "/foo", "foo/", ".foo", "foo.", "a/b/c", "public/foo.bar")) {
            assertThrows<Incorrect>("expected '$str' to be rejected") { TableRef.parse(str) }
        }
    }
}
