package xtdb.table

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.DEFAULT_SCHEMA
import xtdb.api.TableRef
import xtdb.api.error.Incorrect

class TableRefTest {

    @Test
    fun `bare name goes in the default schema`() {
        assertEquals(TableRef(DEFAULT_SCHEMA, "foo"), TableRef.parse("foo"))
    }

    @Test
    fun `dot separates schema from table`() {
        assertEquals(TableRef("information_schema", "columns"), TableRef.parse("information_schema.columns"))
    }

    @Test
    fun `rejects slashes, multiple dots, and empty segments`() {
        for (str in listOf("public/foo", "myschema/foo", "a.b.c", "", "foo.", ".foo")) {
            assertThrows<Incorrect>("expected '$str' to be rejected") { TableRef.parse(str) }
        }
    }
}
