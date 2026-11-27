package xtdb.query

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import xtdb.antlr.Sql

class ParserCacheTest {

    private fun nestedDoc(depth: Int): String =
        (1..depth).fold("1") { inner, level -> if (level % 3 == 0) "[$inner, 2]" else "{a$level: $inner}" }

    private val statements = (1..40).map { "INSERT INTO docs RECORDS {_id: 1, d: ${nestedDoc(it)}}" }

    private fun ParserCache.parseTrees() = statements.map { sql ->
        sql.parseWithFallback(this) { it.directSqlStatement().directlyExecutableStatement() }
            .toStringTree(Sql.ruleNames.toList())
    }

    @Test
    fun `a cache past its limit is replaced, and parses are unchanged`() {
        val unbounded = ParserCache(-1)
        val bounded = ParserCache(200)

        assertEquals(unbounded.parseTrees(), bounded.parseTrees())

        assertEquals(0, unbounded.resetCount)
        assertTrue(unbounded.states > 200, "the corpus outgrows the limit")

        assertTrue(bounded.resetCount > 0)
        assertTrue(bounded.states < unbounded.states)
    }

    @Test
    fun `max states comes from the env var, then the system property, then the default`() {
        assertEquals(ParserCache.DEFAULT_MAX_STATES, ParserCache.maxStates(null, null))
        assertEquals(200, ParserCache.maxStates("200", "300"))
        assertEquals(300, ParserCache.maxStates(null, " 300 "))
        assertEquals(-1, ParserCache.maxStates("-1", null))
        assertEquals(ParserCache.DEFAULT_MAX_STATES, ParserCache.maxStates("200,000", null), "unparseable falls back")
    }
}
