package xtdb.operator.scan

import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.choose
import io.kotest.property.arbitrary.constant
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.XtdbInternal
import xtdb.api.Xtdb
import xtdb.api.log.InMemoryLog
import xtdb.tx.TxOp
import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit
import kotlin.time.Duration.Companion.minutes

/**
 * The node-level counterpart to [AsOfResolverTest], which drives the resolvers directly. Here the same
 * two resolvers are reached through a real node, so the planner's temporal defaults, the pages
 * `filter-pages` admits and the flushed-and-compacted layout are all in the path.
 */
class AsOfResolverNodeTest {

    companion object {
        // a node per iteration, so this runs far fewer than the unit test's
        private const val ITERATIONS = 25

        private val VALID_TIMES = listOf("2018", "2019", "2020", "2021", "2022", "2023")

        private const val COLS = "_id, v, _valid_from, _valid_to, _system_from, _system_to"
    }

    private data class Op(val kind: String, val id: Int, val validFrom: String, val validTo: String?)

    /**
     * [flushAfter] is the op index to flush and compact after, or null not to. Flushing part-way is what
     * splits one entity's events between a flushed trie and the live index.
     */
    private data class Case(val ops: List<Op>, val flushAfter: Int?, val validTime: String, val systemTimeIdx: Int)

    /**
     * One instant per call, a day apart. A transaction's system time is therefore a function of how many
     * have been submitted, not of elapsed time.
     */
    private fun mockClock() = object : InstantSource {
        private var next: Instant = Instant.parse("2020-01-01T00:00:00Z")

        override fun instant(): Instant = next.also { next = it.plus(1, ChronoUnit.DAYS) }
    }

    private fun openNode() = Xtdb.openNode {
        log(InMemoryLog.Factory().instantSource(mockClock()))
        compactor { threads = 0 }
    }

    private fun ts(t: String) = "TIMESTAMP '$t-01-01T00:00:00Z'"

    private fun Op.toSql() = when (kind) {
        "put" -> "INSERT INTO docs (_id, _valid_from, _valid_to, v) VALUES ($id, ${ts(validFrom)}, ${validTo?.let { ts(it) } ?: "NULL"}, $id)"
        "delete" -> "DELETE FROM docs FOR PORTION OF VALID_TIME FROM ${ts(validFrom)} TO ${validTo?.let { ts(it) } ?: "NULL"} WHERE _id = $id"
        else -> "ERASE FROM docs WHERE _id = $id"
    }

    /** Submits one transaction per op — so each gets its own system time — and returns those times. */
    private fun Xtdb.submitOps(ops: List<Op>, flushAfter: Int?): List<Instant> {
        // a DELETE against a table that doesn't exist yet is a planning error, so the table is seeded
        // rather than left to whichever op the generator puts first. Entity 0 is outside the generated range.
        val seeded = listOf(Op("put", 0, VALID_TIMES.first(), null)) + ops

        return seeded.mapIndexed { idx, op ->
            executeTx(listOf(TxOp.Sql(op.toSql()))).systemTime
                .also { if (idx == flushAfter) flushAndCompact() }
        }
    }

    private fun Xtdb.rows(sql: String): Set<Map<*, *>> =
        connect().use { conn ->
            conn.createStatement(sql).use { stmt ->
                stmt.openQuery().use { cursor -> cursor.consume().flatten().toSet() }
            }
        }

    private fun Xtdb.asOfRows(v: String, s: Instant) =
        rows(
            """SELECT $COLS FROM docs
               FOR VALID_TIME AS OF ${ts(v)}
               FOR SYSTEM_TIME AS OF TIMESTAMP '$s'"""
        )

    /**
     * The same question routed through the polygon resolver: widening valid time to ALL takes it off the
     * as-of path, while the system-time bound and the basis stay as the as-of query has them, so both see
     * the same events and treat an erase above the bound alike.
     *
     * Widening system time instead would not be an oracle — the polygon resolver returns before `applyLog`
     * for events above the system bound, so an all-system-time query cuts the same winner into more
     * segments carrying a non-null `_system_to`.
     */
    private fun Xtdb.fullResolutionRows(v: String, s: Instant) =
        rows(
            """SELECT $COLS FROM docs
               FOR ALL VALID_TIME
               FOR SYSTEM_TIME AS OF TIMESTAMP '$s'
               WHERE _valid_from <= ${ts(v)} AND (_valid_to > ${ts(v)} OR _valid_to IS NULL)"""
        )

    private fun Xtdb.flushAndCompact() {
        val db = checkNotNull((this as XtdbInternal).dbCatalog.databaseOrNull("xtdb"))

        db.sendFlushBlockMessage()

        // the flush message is earlier in the log than this transaction, so a synchronous `executeTx`
        // returning means the flush has been processed — no polling needed
        executeTx(listOf(TxOp.Sql("INSERT INTO sentinel (_id) VALUES (1)")))

        db.compactor.compactAllSync(Duration.ofSeconds(30))
    }

    private val opArb: Arb<Op> = arbitrary {
        val vfIdx = Arb.int(0..VALID_TIMES.size - 2).bind()
        val vtIdx = Arb.int(vfIdx + 1..VALID_TIMES.size).bind()

        Op(
            kind = Arb.choose(6 to Arb.constant("put"), 3 to Arb.constant("delete"), 1 to Arb.constant("erase")).bind(),
            id = Arb.int(1..4).bind(),
            validFrom = VALID_TIMES[vfIdx],
            // one past the end stands for an open-ended range
            validTo = VALID_TIMES.getOrNull(vtIdx)
        )
    }

    private val caseArb: Arb<Case> = arbitrary {
        val ops = Arb.list(opArb, 1..12).bind()

        Case(
            ops = ops,
            flushAfter = Arb.choose(1 to Arb.constant(null), 2 to Arb.int(0..ops.size)).bind(),
            validTime = Arb.element(VALID_TIMES).bind(),
            systemTimeIdx = Arb.int(0..99).bind()
        )
    }

    private fun check(case: Case) {
        openNode().use { node ->
            val sysTimes = node.submitOps(case.ops, case.flushAfter)

            val s = sysTimes[case.systemTimeIdx % sysTimes.size]

            assertEquals(node.fullResolutionRows(case.validTime, s), node.asOfRows(case.validTime, s), "$case")
        }
    }

    @Test
    fun `an erase above the system-time bound still hides the entity`() =
        check(
            Case(
                ops = listOf(
                    Op("put", 1, "2018", null),
                    Op("put", 1, "2020", "2021"),
                    Op("erase", 1, "2018", null)
                ),
                flushAfter = null, validTime = "2020", systemTimeIdx = 1
            )
        )

    @Test
    fun `neighbouring ranges bracket the winner's valid time across a block flush`() =
        check(
            Case(
                ops = listOf(
                    Op("put", 1, "2018", null),
                    Op("put", 1, "2019", "2020"),
                    Op("put", 1, "2022", "2023")
                ),
                flushAfter = 3, validTime = "2021", systemTimeIdx = 3
            )
        )

    @Test
    fun `one entity's events span the live index and a flushed trie`() =
        check(
            Case(
                ops = listOf(
                    Op("put", 1, "2018", null),
                    Op("put", 1, "2019", "2020"),
                    // flushed above this line, so the merge has to span a trie and the live index
                    Op("put", 1, "2021", "2022")
                ),
                flushAfter = 2, validTime = "2019", systemTimeIdx = 3
            )
        )

    @Tag("property")
    @Test
    fun `as-of resolution agrees through page filtering and compaction`() = runTest(timeout = 2.minutes) {
        checkAll(ITERATIONS, caseArb) { case -> check(case) }
    }
}
