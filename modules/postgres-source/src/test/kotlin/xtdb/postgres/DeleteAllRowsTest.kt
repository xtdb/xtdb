package xtdb.postgres

import clojure.lang.Keyword
import org.junit.jupiter.api.Test
import xtdb.api.error.Incorrect
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DeleteAllRowsTest {

    @Test
    fun `deletes from the quoted table, keeping its case`() {
        assertEquals("""DELETE FROM "public"."Orders"""", deleteAllRowsSql("public", "Orders", forAllValidTime = false))
    }

    @Test
    fun `deletes for all valid time when asked`() {
        assertEquals("""DELETE FROM "public"."orders" FOR ALL VALID_TIME""", deleteAllRowsSql("public", "orders", forAllValidTime = true))
    }

    @Test
    fun `refuses an identifier XT SQL can't quote`() {
        for ((schema, table) in listOf("public" to "a\"b", "public" to "a/b", "s\"x" to "orders")) {
            val e = assertFailsWith<Incorrect> { deleteAllRowsSql(schema, table, forAllValidTime = false) }
            assertEquals(Keyword.intern("xtdb.postgres", "unquotable-identifier"), e.data.valAt(Keyword.intern("xtdb.error", "code")))
        }
    }
}
