package xtdb.postgres

import io.kotest.property.Arb
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import xtdb.api.Xtdb
import java.math.BigDecimal
import java.nio.file.Files
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Fuzzes CDC type fidelity and bitemporal update-history over a real Postgres container:
 * generate a schema of typed columns + rows, push them through the CDC pipeline, and assert
 * each value round-trips into XTDB unchanged (read back over pgwire) — both while the block is
 * still in memory and after it's flushed to storage. Failures shrink to the minimal set of
 * column types/values.
 */
@Tag("property")
class PostgresSourceTypesPropertyTest : PostgresSourceTestBase() {

    companion object {
        private const val ITERATIONS = 50

        // --- generator bounds ---
        // NUMERIC: <=9 significant and <=9 fractional digits keeps the column's merged precision
        // well under XTDB's max of 64, while still spanning whole and fractional values.
        private const val MAX_NUMERIC_UNSCALED = 999_999_999L   // nine 9s
        private const val MAX_NUMERIC_SCALE = 9
        // DATE: epoch day 0..50_000 ≈ 1970-01-01 .. ~2106, comfortably inside PG's date range.
        private const val MAX_EPOCH_DAY = 50_000L
        private const val LAST_SECOND_OF_DAY = 86_399L          // 24 * 60 * 60 - 1
        // TIMESTAMPTZ: well within the ±18h ZoneOffset limit; ±12h covers every real-world zone.
        private const val MAX_TZ_OFFSET_HOURS = 12
        private const val MAX_TEXT_LEN = 32
        private const val MAX_COL_NAME_LEN = 20

        // Anchor for explicit `_valid_from` histories: each update steps one day on from here, so
        // the valid-times are distinct and strictly ascending regardless of the generated data.
        private val VALID_FROM_BASE: OffsetDateTime = OffsetDateTime.parse("2020-01-01T00:00:00Z")
    }

    // --- generators -----------------------------------------------------------------------

    /** A PG column type paired with a generator for values that pgjdbc's `setObject` takes directly. */
    private data class PgType(val ddl: String, val arb: Arb<out Any>)

    private val boundedDecimal: Arb<BigDecimal> =
        Arb.bind(
            Arb.long(-MAX_NUMERIC_UNSCALED..MAX_NUMERIC_UNSCALED), Arb.int(0..MAX_NUMERIC_SCALE)
        ) { unscaled, scale -> BigDecimal.valueOf(unscaled).movePointLeft(scale) }

    // Integer-second granularity: PG's temporal columns are microsecond-precision, so dropping
    // sub-seconds means values come back bit-for-bit and there's no rounding to reconcile.
    private val localDate: Arb<LocalDate> = Arb.long(0L..MAX_EPOCH_DAY).map(LocalDate::ofEpochDay)
    private val localTime: Arb<LocalTime> = Arb.long(0L..LAST_SECOND_OF_DAY).map(LocalTime::ofSecondOfDay)
    private val localDateTime: Arb<LocalDateTime> =
        Arb.bind(localDate, localTime) { d, t -> LocalDateTime.of(d, t) }
    private val offsetDateTime: Arb<OffsetDateTime> =
        Arb.bind(localDateTime, Arb.int(-MAX_TZ_OFFSET_HOURS..MAX_TZ_OFFSET_HOURS)) { ldt, h ->
            OffsetDateTime.of(ldt, ZoneOffset.ofHours(h))
        }

    // The set of column types a generated schema draws from — one PG type per XTDB scalar we want
    // to exercise, each with its value generator. A schema picks a random type per column (repeats
    // allowed), so a single run can stress several columns of the same type.
    private val typePool = listOf(
        PgType("SMALLINT", Arb.short()),
        PgType("INTEGER", Arb.int()),
        PgType("BIGINT", Arb.long()),
        // finite only: NaN/±Inf are representable in PG doubles but aren't worth the equality
        // special-casing, and aren't what this test is about
        PgType("DOUBLE PRECISION", Arb.double().filter { it.isFinite() }),
        PgType("NUMERIC", boundedDecimal),
        // alphanumeric rather than arbitrary UTF-8: PG rejects NUL (0x00) bytes in TEXT
        PgType("TEXT", Arb.string(0..MAX_TEXT_LEN, Codepoint.alphanumeric())),
        PgType("BOOLEAN", Arb.boolean()),
        PgType("UUID", Arb.uuid()),
        PgType("TIMESTAMPTZ", offsetDateTime),
        PgType("TIMESTAMP", localDateTime),
        PgType("TIME", localTime),
        PgType("DATE", localDate),
    )

    private data class Col(val name: String, val ddl: String, val arb: Arb<out Any>)

    // "c" + lowercase alphanumerics: always a valid identifier — can't lead with a digit or collide
    // with a reserved word, and lowercasing sidesteps PG's quoting / case-folding rules
    private val colName: Arb<String> =
        Arb.string(1..MAX_COL_NAME_LEN, Codepoint.alphanumeric()).map { "c" + it.lowercase() }

    // n columns with distinct names, each assigned a random type from the pool
    private fun schema(minCols: Int, maxCols: Int): Arb<List<Col>> = arbitrary {
        val n = Arb.int(minCols..maxCols).bind()
        val names = Arb.set(colName, n..n).bind().toList()
        val types = Arb.list(Arb.element(typePool), n..n).bind()
        names.zip(types) { name, type -> Col(name, type.ddl, type.arb) }
    }

    // one generated value per column (a full row's worth), keyed by column name
    private fun rowValues(cols: List<Col>): Arb<Map<String, Any?>> =
        arbitrary { cols.associate { it.name to it.arb.bind() } }

    private data class Row(val id: Long, val vals: Map<String, Any?>)
    private data class SchemaRows(val columns: List<Col>, val rows: List<Row>)

    // a schema plus 1..maxRows rows over it, each with a distinct _id
    private fun schemaRows(maxRows: Int, maxCols: Int): Arb<SchemaRows> = arbitrary {
        val columns = schema(1, maxCols).bind()
        val rowVals = Arb.list(rowValues(columns), 1..maxRows).bind()
        val ids = Arb.set(Arb.long(), rowVals.size..rowVals.size).bind().toList()
        SchemaRows(columns, rowVals.zip(ids) { vals, id -> Row(id, vals) })
    }

    private data class History(val columns: List<Col>, val states: List<Map<String, Any?>>)

    // a schema plus a sequence of full row-states for a single _id: the first inserts the row, each
    // subsequent one rewrites every column. With `withValidFrom`, appends a `_valid_from` TIMESTAMPTZ
    // column carrying a distinct, ascending explicit valid-time per state — exercising the source
    // honouring `_valid_from` (PostgresSource.writeOp) instead of deriving it from the commit time.
    private fun history(maxCols: Int, maxUpdates: Int, withValidFrom: Boolean = false): Arb<History> = arbitrary {
        val columns = schema(1, maxCols).bind()
        val states = Arb.list(rowValues(columns), 1..maxUpdates).bind()
        // XTDB dedups an unchanged put, so an identical successive state yields no new valid-time
        // version — drop consecutive duplicates so "one version per commit" actually holds. Dedup on
        // the data columns only; _valid_from is consumed by the source, not stored as content.
        val changing = states.filterIndexed { i, s ->
            i == 0 || normalizeRow(columns, states[i - 1]) != normalizeRow(columns, s)
        }
        if (!withValidFrom) return@arbitrary History(columns, changing)

        // step one day per update from the base so valid-times are distinct and ascending (commit
        // order == valid-time order → clean [Vᵢ, Vᵢ₊₁) segments). Injected by index rather than drawn
        // from the column's arb, since the ascending-across-the-sequence constraint is cross-state.
        History(
            columns + Col("_valid_from", "TIMESTAMPTZ", offsetDateTime),
            changing.mapIndexed { i, s -> s + ("_valid_from" to VALID_FROM_BASE.plusDays(i.toLong())) },
        )
    }

    // --- comparison -----------------------------------------------------------------------

    /** Collapses representation differences between what we set via pgjdbc and what comes back
     * over pgwire: offset/zoned timestamps to their instant, decimals to scale-insensitive form
     * (pgwire doesn't preserve NUMERIC scale), and integral widths to Long. */
    private fun normalize(v: Any?): Any? = when (v) {
        is OffsetDateTime -> v.toInstant()
        is ZonedDateTime -> v.toInstant()
        is BigDecimal -> v.stripTrailingZeros().let { if (it.signum() == 0) BigDecimal.ZERO else it }
        // `+ 0.0` canonicalises -0.0 to 0.0 so it matches XTDB's equality (and so the history
        // generator doesn't treat a -0.0/0.0 step as a real change XTDB would then dedup away)
        is Float -> v.toDouble() + 0.0
        is Double -> v + 0.0
        is Byte, is Short, is Int, is Long -> (v as Number).toLong()
        else -> v
    }

    private fun normalizeRow(cols: List<Col>, vals: Map<String, Any?>) =
        cols.associate { it.name to normalize(vals[it.name]) }

    // --- writes -----------------------------------------------------------------------------

    private fun insertRow(cols: List<Col>, table: String, row: Row) {
        val names = listOf("_id") + cols.map { it.name }
        val sql = "INSERT INTO $table (${names.joinToString()}) VALUES (${names.joinToString { "?" }})"
        commitTx(sql) { ps ->
            ps.setObject(1, row.id)
            cols.forEachIndexed { i, col -> ps.setObject(i + 2, row.vals[col.name]) }
        }
    }

    private fun updateRow(cols: List<Col>, table: String, id: Long, vals: Map<String, Any?>) {
        val sql = "UPDATE $table SET ${cols.joinToString { "${it.name} = ?" }} WHERE _id = ?"
        commitTx(sql) { ps ->
            cols.forEachIndexed { i, col -> ps.setObject(i + 1, vals[col.name]) }
            ps.setObject(cols.size + 1, id)
        }
    }

    /** Creates a table + publication, runs `beforeAttach` (e.g. to seed rows the initial snapshot
     *  should pick up), opens a local-log/storage node, attaches the cdc db, waits for streaming to
     *  go live, runs `f`, then drops the replication slot.
     *
     *  `suspend inline` (with non-suspend lambdas) so both this body and the inlined lambdas can
     *  call the `suspend` `awaitStreaming` / `awaitCondition` / `flushBlock` helpers inside
     *  `checkAll`'s suspend block. */
    private suspend inline fun runCdc(
        columns: List<Col>,
        beforeAttach: (String) -> Unit = {},
        f: (Xtdb, String) -> Unit,
    ) {
        val table = unique("t"); val pub = unique("pub"); val slot = unique("slot")
        val colDdl = columns.joinToString { "${it.name} ${it.ddl}" }
        pgExecute(
            "CREATE TABLE $table (_id BIGINT PRIMARY KEY, $colDdl)",
            "CREATE PUBLICATION $pub FOR TABLE $table",
        )
        beforeAttach(table)
        val dirs = List(4) { Files.createTempDirectory("pg-prop") }
        try {
            openNode(dirs[0], dirs[1]).use { node ->
                attachCdc(node, dirs[2], dirs[3], slot, pub)
                awaitStreaming(node)
                f(node, table)
            }
        } finally {
            dropSlot(slot)
            dirs.forEach { it.toFile().deleteRecursively() }
        }
    }

    /** Awaits all `rows` appearing in `table` (by `_id`) over pgwire, then asserts every column
     *  value round-trips unchanged. Shared by the streaming and snapshot round-trip tests. */
    private suspend fun assertRowsPresent(
        node: Xtdb, table: String, columns: List<Col>, rows: List<Row>, stage: String,
    ) {
        val cols = (listOf("_id") + columns.map { it.name }).joinToString()
        val ids = rows.map { it.id }.toSet()
        fun fetch() = xtQuery(node, "SELECT $cols FROM public.$table")
            .associateBy { (it["_id"] as Number).toLong() }

        awaitCondition("rows appear in $table ($stage)", timeout = 30.seconds) {
            fetch().keys.containsAll(ids)
        }
        val actual = fetch()
        rows.forEach { row ->
            assertEquals(
                normalizeRow(columns, row.vals), normalizeRow(columns, actual.getValue(row.id)),
                "row ${row.id} $stage",
            )
        }
    }

    /** Awaits the FOR ALL VALID_TIME history of `_id = 1` (ordered by `_valid_from`) and asserts it
     *  matches `states` one-version-per-state, every column round-tripping. Shared by the commit-time
     *  and explicit-`_valid_from` history tests; when `states` carry a `_valid_from` column it's just
     *  another column to compare, so the version's valid-time is checked against what we set. */
    private suspend fun assertHistoryVersions(
        node: Xtdb, table: String, columns: List<Col>, states: List<Map<String, Any?>>, stage: String,
    ) {
        val cols = columns.joinToString { it.name }
        val q = "SELECT $cols FROM public.$table FOR ALL VALID_TIME WHERE _id = 1 ORDER BY _valid_from"
        val expected = states.map { normalizeRow(columns, it) }
        // assert on the awaited value (not a bare awaitCondition) so a shortfall reports exactly which
        // versions showed up vs the expected list, rather than just timing out
        val actual = await(done = { it.size == expected.size }) { xtQuery(node, q).map { normalizeRow(columns, it) } }
        assertEquals(expected, actual, "$stage — expected ${expected.size} versions, got ${actual.size}")
    }

    // --- property tests -----------------------------------------------------------------------

    @Test
    fun `every column type round-trips through CDC unchanged`() = runTest(timeout = 30.minutes) {
        checkAll(ITERATIONS, schemaRows(maxRows = 20, maxCols = 5)) { (columns, rows) ->
            runCdc(columns) { node, table ->
                rows.forEach { insertRow(columns, table, it) }
                assertRowsPresent(node, table, columns, rows, "after insert")
                flushBlock(node)
                assertRowsPresent(node, table, columns, rows, "after block flush")
            }
        }
    }

    // Exercises the snapshot path specifically: rows are inserted before attach, so the source's
    // initial snapshot (SET TRANSACTION SNAPSHOT + text coercion) is what ingests them — a distinct
    // code path from the streaming pgoutput decode the test above covers.
    @Test
    fun `rows inserted before attach are captured by the initial snapshot`() = runTest(timeout = 30.minutes) {
        checkAll(ITERATIONS, schemaRows(maxRows = 20, maxCols = 5)) { (columns, rows) ->
            runCdc(columns, beforeAttach = { table -> rows.forEach { insertRow(columns, table, it) } }) { node, table ->
                assertRowsPresent(node, table, columns, rows, "after snapshot")
                flushBlock(node)
                assertRowsPresent(node, table, columns, rows, "after block flush")
            }
        }
    }

    @Test
    fun `successive commits each land as a distinct valid-time version`() = runTest(timeout = 30.minutes) {
        checkAll(ITERATIONS, history(maxCols = 4, maxUpdates = 8)) { (columns, states) ->
            runCdc(columns) { node, table ->
                insertRow(columns, table, Row(1, states.first()))
                states.drop(1).forEach { updateRow(columns, table, 1, it) }

                assertHistoryVersions(node, table, columns, states, "every committed row-state is a distinct version")
                flushBlock(node)
                assertHistoryVersions(node, table, columns, states, "history intact after block flush")
            }
        }
    }

    @Test
    fun `an explicit _valid_from column sets each version's valid-time`() = runTest(timeout = 30.minutes) {
        checkAll(ITERATIONS, history(maxCols = 4, maxUpdates = 8, withValidFrom = true)) { (columns, states) ->
            runCdc(columns) { node, table ->
                insertRow(columns, table, Row(1, states.first()))
                states.drop(1).forEach { updateRow(columns, table, 1, it) }

                assertHistoryVersions(node, table, columns, states, "each version lands at its explicit _valid_from")
                flushBlock(node)
                assertHistoryVersions(node, table, columns, states, "history intact after block flush")
            }
        }
    }
}
