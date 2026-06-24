package xtdb.datasets.edgar

import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.postgres.PgIndexer
import xtdb.postgres.PostgresDriver
import xtdb.postgres.RowOp
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.ZoneOffset

/**
 * A [PgIndexer] for EDGAR fundamentals loaded into Postgres by
 * `xtdb.datasets.edgar.pg`.
 *
 * The Postgres tables are current-state — keyed by their natural ids, with no `_id` and no
 * `_valid_from`, and a latest-`filed`-wins UPSERT. This indexer derives both from each row's own
 * columns, so the XTDB side accrues the bitemporal history the source-of-record never held: every
 * filing that streams through Postgres lands as its own system-time version, and a restatement
 * supersedes the prior belief rather than overwriting it.
 *
 *  - `_id` matches the native [xtdb.datasets.edgar] loader exactly (so a native bulk load and a CDC
 *    stream produce identical identities): `cik` (issuer);
 *    `cik__income_statement__{start}__{end}` (income_statement);
 *    `cik__balance_sheet__{end}` (balance_sheet).
 *  - `_valid_from`: `filed` for issuer/income_statement (the figure is fixed for its period, so a
 *    correction lives on the system axis); `period_end` for balance_sheet (an instant balance is
 *    as-of that date — a real valid-time timeline).
 *
 * Dates are derived from the row's `date` columns via [LocalDate], whose canonical ISO-8601
 * toString matches the ids the native loader writes — deriving the id from the column's own
 * rendering would risk a different string for the same value and split the identity.
 *
 * A plain, code-supplied [PgIndexer.Factory] with no Registration: it travels in an
 * [xtdb.api.IngestNode] config, never persisted as serialised secondary-database config. Wire it as
 * GleifPgIndexer documents, with the EDGAR publication/slot.
 */
class EdgarPgIndexer(private val dbName: String) : PgIndexer {

    override fun indexTx(tx: PostgresDriver.Transaction, openTx: OpenTx) {
        for (op in tx.ops) writeOp(openTx, op)
    }

    private fun writeOp(openTx: OpenTx, op: RowOp) {
        val table = openTx.table(TableRef(dbName, op.schema, op.table))

        when (op) {
            is RowOp.Put -> {
                val id = deriveId(op.table, op.row)
                val doc = op.row + ("_id" to id)
                val validFrom = validFromMicros(op.table, op.row)
                table.logPut(ByteBuffer.wrap(id.asIid), validFrom, Long.MAX_VALUE) {
                    table.docWriter.writeObject(doc)
                }
            }

            is RowOp.Delete -> {
                val id = deriveId(op.table, op.row)
                table.logDelete(ByteBuffer.wrap(id.asIid), openTx.systemFrom, Long.MAX_VALUE)
            }
        }
    }

    /** Natural key per table, matching the ids the native XT loader writes. */
    private fun deriveId(table: String, row: Map<String, Any?>): String = when (table) {
        "issuer" -> row.str("cik", table)
        "income_statement" ->
            "${row.str("cik", table)}__income_statement__" +
                "${row.date("period_start", table)}__${row.date("period_end", table)}"
        "balance_sheet" ->
            "${row.str("cik", table)}__balance_sheet__${row.date("period_end", table)}"

        else -> throw Incorrect(
            "EdgarPgIndexer doesn't handle table '$table'",
            errorCode = "xtdb.datasets.edgar/unknown-table",
            data = mapOf("table" to table),
        )
    }

    /** issuer + income_statement: valid-from = filed; balance_sheet: = period_end. */
    private fun validFromMicros(table: String, row: Map<String, Any?>): Long = when (table) {
        "balance_sheet" -> row.date("period_end", table).startOfDayUtcMicros()
        else -> row.date("filed", table).startOfDayUtcMicros()
    }

    class Factory : PgIndexer.Factory {
        override fun open(dbName: String): PgIndexer = EdgarPgIndexer(dbName)
        override fun equals(other: Any?) = other is Factory
        override fun hashCode() = javaClass.hashCode()
    }

    private companion object {
        fun LocalDate.startOfDayUtcMicros(): Long = atStartOfDay(ZoneOffset.UTC).toInstant().asMicros

        fun Map<String, Any?>.str(col: String, table: String): String =
            this[col]?.toString()
                ?: throw Incorrect(
                    "missing '$col' in row from $table",
                    errorCode = "xtdb.datasets.edgar/missing-column",
                    data = mapOf("column" to col, "table" to table),
                )

        // pgjdbc maps a `date` column to java.sql.Date or java.time.LocalDate; accept both.
        fun Map<String, Any?>.date(col: String, table: String): LocalDate = when (val v = this[col]) {
            is LocalDate -> v
            is java.sql.Date -> v.toLocalDate()
            else -> throw Incorrect(
                "'$col' must be a non-null DATE column",
                errorCode = "xtdb.datasets.edgar/invalid-date",
                data = mapOf(
                    "column" to col, "table" to table,
                    "type" to (v?.let { it::class.qualifiedName ?: it::class.simpleName }),
                ),
            )
        }
    }
}
