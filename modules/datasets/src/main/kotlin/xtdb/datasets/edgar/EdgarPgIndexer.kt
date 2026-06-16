package xtdb.datasets.edgar

import com.google.protobuf.Empty
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.error.Incorrect
import xtdb.api.tx.OpenTx
import xtdb.postgres.PgIndexer
import xtdb.postgres.PostgresDriver
import xtdb.postgres.RowOp
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import com.google.protobuf.Any as ProtoAny

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
 * It carries a `!Edgar` [PgIndexer.Registration] so the factory can travel in `ATTACH DATABASE`
 * config — letting a full, queryable node run it (the demo queries the EDGAR node over pgwire, which
 * an [xtdb.api.IngestNode] wouldn't have).
 *
 * Wiring it in: start a node with a `pg` remote (a `:postgres` `:remotes` entry, or the Kotlin
 * builder) and ATTACH the database, selecting this indexer by its registered tag:
 *
 * ```sql
 * ATTACH DATABASE edgar WITH $$
 *   log: !Kafka { cluster: 'kafka', topic: 'xtdb.edgar' }
 *   storage: !Local { path: '/tmp/edgar-demo/edgar' }
 *   externalSource: !Postgres
 *     remote: 'pg'
 *     slotName: 'edgar_slot'
 *     publicationName: 'edgar_pub'
 *     indexer: !Edgar {}
 * $$
 * ```
 *
 * See modules/datasets/edgar/README.md, and the `(comment ...)` at the bottom of the
 * xtdb.datasets.edgar namespace, for the end-to-end run.
 */
class EdgarPgIndexer : PgIndexer {

    override fun indexTx(tx: PostgresDriver.Transaction, openTx: OpenTx) {
        for (op in tx.ops) writeOp(openTx, op)
    }

    private fun writeOp(openTx: OpenTx, op: RowOp) {
        val table = openTx.table(op.schema, op.table)

        when (op) {
            is RowOp.Put -> {
                val id = deriveId(op.table, op.row)
                val doc = op.row + ("_id" to id)
                table.writePut(doc, validFrom = validFrom(op.table, op.row))
            }

            is RowOp.Delete -> table.writeDelete(deriveId(op.table, op.row))
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
    private fun validFrom(table: String, row: Map<String, Any?>): Instant = when (table) {
        "balance_sheet" -> row.date("period_end", table).startOfDayUtc()
        else -> row.date("filed", table).startOfDayUtc()
    }

    @Serializable
    @SerialName("!Edgar")
    class Factory : PgIndexer.Factory {
        override fun open(): PgIndexer = EdgarPgIndexer()
        override fun equals(other: Any?) = other is Factory
        override fun hashCode() = javaClass.hashCode()

        // Registered (META-INF/services) so the factory can travel as persisted
        // secondary-database config — the demo queries the EDGAR node over pgwire,
        // which needs a full node (ATTACH DATABASE), not an un-persisted IngestNode.
        // No config fields, so the proto carrier is an empty well-known message —
        // but *not* google.protobuf.Struct: GLEIF already claims that type-URL, and
        // it's the sole discriminator in the persisted path (PgIndexer.Factory.fromProto
        // keys on it), so a shared carrier would let one db deserialise as the other.
        // Empty gives EDGAR a distinct type-URL with no generated message.
        class Registration : PgIndexer.Registration<Factory> {
            override val protoTag get() = "proto.xtdb.com/google.protobuf.Empty"
            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(Empty.getDefaultInstance(), "proto.xtdb.com")

            override fun fromProto(msg: ProtoAny): PgIndexer.Factory {
                msg.unpack(Empty::class.java)
                return Factory()
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<PgIndexer.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    private companion object {
        fun LocalDate.startOfDayUtc(): Instant = atStartOfDay(ZoneOffset.UTC).toInstant()

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
