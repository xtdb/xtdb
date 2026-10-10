package xtdb.compactor

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.api.log.InMemoryLog
import xtdb.tx.TxOp
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit

class SameTransactionEventsTest {

    private data class Case(val name: String, val statements: List<String>)

    private fun puts(first: Pair<String, String>?, second: Pair<String, String>?) =
        listOf(insert(1, first), insert(2, second))

    private val erase = "ERASE FROM docs WHERE _id = 1"

    private val cases = listOf(
        Case("same valid-time", puts(null, null)),
        Case("overridden in valid-time", puts("2023" to "2024", "2022" to "2025")),
        Case("split in valid-time", puts("2022" to "2025", "2023" to "2024")),
        Case("overlapping valid-time", puts("2022" to "2025", "2023" to "2026")),
        Case("the later event ends in the past", puts("2019" to "2025", "2020" to "2021")),
        Case("an erase after the puts", puts(null, null) + erase),
        Case("an erase before the puts", listOf(erase) + puts(null, null)),
    )

    private val queries = listOf(
        "SELECT *, _valid_from, _valid_to, _system_from, _system_to FROM docs FOR ALL VALID_TIME",
        "SELECT *, _valid_from, _valid_to, _system_from, _system_to FROM docs FOR ALL VALID_TIME FOR ALL SYSTEM_TIME",
    )

    private fun mockClock() = object : InstantSource {
        private var next: Instant = Instant.parse("2020-01-01T00:00:00Z")
        override fun instant(): Instant = next.also { next = it.plus(1, ChronoUnit.DAYS) }
    }

    private fun openNode() = Xtdb.openNode {
        log(InMemoryLog.Factory().instantSource(mockClock()))
        compactor { threads = 0 }
    }

    private fun insert(version: Int, validTime: Pair<String, String>?) =
        if (validTime == null) "INSERT INTO docs (_id, version) VALUES (1, $version)"
        else "INSERT INTO docs (_id, _valid_from, _valid_to, version) VALUES " +
                "(1, TIMESTAMP '${validTime.first}-01-01T00:00:00Z', TIMESTAMP '${validTime.second}-01-01T00:00:00Z', $version)"

    private fun Xtdb.rows(sql: String): Set<Map<*, *>> =
        connect().use { conn ->
            conn.createStatement(sql).use { stmt ->
                stmt.openQuery().use { cursor -> cursor.consume().flatten().toSet() }
            }
        }

    private fun Xtdb.indexCase(case: Case) {
        executeTx(listOf(TxOp.Sql("INSERT INTO docs (_id, version) VALUES (0, 0)")))
        executeTx(case.statements.map { TxOp.Sql(it) })
    }

    private fun Xtdb.flushBlock() {
        checkNotNull((this as XtdbInternal).dbCatalog.databaseOrNull("xtdb")).sendFlushBlockMessage()
        executeTx(listOf(TxOp.Sql("INSERT INTO sentinel (_id) VALUES (1)")))
    }

    private fun Xtdb.compactAll() {
        checkNotNull((this as XtdbInternal).dbCatalog.databaseOrNull("xtdb")).compactor.compactAllSync(Duration.ofSeconds(30))
    }

    @Test
    fun `same-transaction events in an unmerged L0 file query as they did from the live index`() {
        for (case in cases) {
            openNode().use { node ->
                node.indexCase(case)
                val fromLiveIndex = queries.map { node.rows(it) }

                node.flushBlock()

                assertEquals(fromLiveIndex, queries.map { node.rows(it) }, "${case.name}: L0")
            }
        }
    }

    @Test
    fun `same-transaction events compacted from L0 to L1 query as they did from the live index`() {
        for (case in cases) {
            openNode().use { node ->
                node.indexCase(case)
                val fromLiveIndex = queries.map { node.rows(it) }

                node.flushBlock()
                node.compactAll()

                assertEquals(fromLiveIndex, queries.map { node.rows(it) }, "${case.name}: L1")
            }
        }
    }
}
