package xtdb.query

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.query.ParsedStatement.*

class SqlParserTest {

    private fun one(sql: String): ParsedStatement = parseStatements(sql).single()

    @Test
    fun `classifies queries and SQL-answered shows`() {
        assertTrue(one("SELECT 1") is Query)
        assertTrue(one("SHOW TIME ZONE") is Query)
        assertTrue(one("SHOW SNAPSHOT_TOKEN") is Query)
    }

    @Test
    fun `classifies dml by statement type`() {
        assertTrue(one("INSERT INTO foo (a) VALUES (1)") is Dml.Insert)
        assertTrue(one("UPDATE foo SET a = 1") is Dml.Update)
        assertTrue(one("DELETE FROM foo") is Dml.Delete)
        assertTrue(one("ERASE FROM foo") is Dml.Erase)
        assertTrue(one("ASSERT 1 = 1") is Dml.Assert)
    }

    @Test
    fun `grant and revoke carry user and role`() {
        val grant = one("GRANT r TO u") as Dml.GrantRole
        assertEquals("u", grant.user)
        assertEquals("r", grant.role)

        val revoke = one("REVOKE r FROM u") as Dml.RevokeRole
        assertEquals("u", revoke.user)
        assertEquals("r", revoke.role)
    }

    @Test
    fun `create table carries name parts and columns`() {
        val ct = one("CREATE TABLE my_schema.foo (a, b)") as Dml.CreateTable
        assertEquals("my_schema", ct.schema)
        assertEquals("foo", ct.table)
        assertEquals(listOf("a", "b"), ct.colNames)
    }

    @Test
    fun `transaction lifecycle and access mode`() {
        assertEquals(AccessMode.READ_ONLY, (one("BEGIN READ ONLY") as Begin).txOptions.accessMode)
        assertEquals(AccessMode.READ_WRITE, (one("BEGIN READ WRITE") as Begin).txOptions.accessMode)
        assertTrue(one("COMMIT") is Commit)
        assertTrue(one("ROLLBACK") is Rollback)
    }

    @Test
    fun `session setters carry their values`() {
        assertTrue(one("SET TIME ZONE 'UTC'") is SetTimeZone)
        assertTrue(one("SET AWAIT_TOKEN = 'tok'") is SetAwaitToken)
        assertEquals("my_var", (one("SET my_var = 1") as SetSessionParameter).name)
        assertTrue(one("SET ROLE NONE") is SetRole)
    }

    @Test
    fun `show variables answered from session state`() {
        assertEquals("my_var", (one("SHOW my_var") as ShowVariable).variable)
        assertEquals("await_token", (one("SHOW AWAIT_TOKEN") as ShowVariable).variable)
    }

    @Test
    fun `copy-in parses table and format enum`() {
        val copy = one("COPY foo FROM STDIN WITH (FORMAT 'arrow-stream')") as CopyIn
        assertEquals("foo", copy.table)
        assertEquals(CopyFormat.ARROW_STREAM, copy.format)

        assertNull((one("COPY foo FROM STDIN WITH (FORMAT 'bogus')") as CopyIn).format)
    }

    @Test
    fun `attach carries db name and config yaml text`() {
        assertEquals("mydb", (one("ATTACH DATABASE mydb") as AttachDatabase).dbName)
        assertEquals("k: v", (one("ATTACH DATABASE mydb WITH 'k: v'") as AttachDatabase).configYaml)
        assertEquals("mydb", (one("DETACH DATABASE mydb") as DetachDatabase).dbName)
    }

    @Test
    fun `prepare nests its inner statement, execute names it`() {
        val p = one("PREPARE p AS SELECT 1") as Prepare
        assertEquals("p", p.name)
        assertTrue(p.inner is Query)
        assertEquals("p", (one("EXECUTE p") as Execute).name)
    }

    @Test
    fun `originalSql is the per-statement source slice`() {
        val stmts = parseStatements("SELECT 1; INSERT INTO foo (a) VALUES (1)")
        assertEquals(2, stmts.size)
        assertTrue(stmts[0] is Query)
        assertTrue(stmts[1] is Dml.Insert)
        assertEquals("SELECT 1", stmts[0].originalSql)
        assertEquals("INSERT INTO foo (a) VALUES (1)", stmts[1].originalSql)
    }
}
