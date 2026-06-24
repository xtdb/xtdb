package xtdb.datasets.gleif

import com.google.protobuf.Struct
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.kw
import xtdb.postgres.PgIndexer
import xtdb.postgres.PostgresDriver
import xtdb.postgres.RowOp
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import com.google.protobuf.Any as ProtoAny

/**
 * A [PgIndexer] for GLEIF reference data loaded into Postgres by `xtdb.datasets.gleif.pg`.
 *
 * The Postgres tables are current-state — keyed by their natural ids, with no `_id` and no
 * `_valid_from`. This indexer derives both from the row's own columns, so the XTDB side accrues a
 * bitemporal history the non-bitemporal source-of-record never held — the CDC stream *is* how XTDB
 * gets populated (there's no native XTDB loader). Three table families:
 *
 *  - `lei` (the entity): `_id` = `lei`, valid from system-time — current state;
 *  - `*_changes` (state transitions, e.g. `lei_legal_name_changes`): `_id` = `lei`, valid from
 *    `effective_date`. XT's effective PK is `_id` + `_valid_from`, so successive changes to one
 *    entity are versions of one document — the bitemporal name/address history;
 *  - everything else (occurrences, e.g. `lei_dissolutions`): `_id` = `lei__effective_date`, valid
 *    from `effective_date` — an entity can have several, so each is its own document.
 *
 * Postgres enum columns arrive as their kebab text label; we re-key them to keyword values (the
 * loader stores `:active`, not `"active"`). jsonb columns (addresses, other-names) arrive already
 * decoded as maps — string-keyed, but that collapses to the same struct field in XT, so they pass
 * through as-is.
 *
 * It carries a `!Gleif` [PgIndexer.Registration] so the factory can travel in `ATTACH DATABASE`
 * config — letting a full, queryable node run it. (An [xtdb.api.IngestNode] could run it
 * un-persisted too, but has no pgwire.)
 *
 * Wiring it in: start a node with a `pg` remote (a `:postgres` `:remotes` entry, or the Kotlin
 * builder) and ATTACH the database, selecting this indexer by its registered tag:
 *
 * ```sql
 * ATTACH DATABASE gleif WITH $$
 *   log: !Kafka { cluster: 'kafka', topic: 'xtdb.gleif' }
 *   storage: !Local { path: '/tmp/xt26-demo/gleif' }
 *   externalSource: !Postgres
 *     remote: 'pg'
 *     slotName: 'gleif_slot'
 *     publicationName: 'gleif_pub'
 *     indexer: !Gleif {}
 * $$
 * ```
 *
 * See the `xt26` dev namespace for the end-to-end run.
 */
class GleifPgIndexer(private val dbName: String) : PgIndexer {

    override fun indexTx(tx: PostgresDriver.Transaction, openTx: OpenTx) {
        for (op in tx.ops) writeOp(openTx, op)
    }

    private fun writeOp(openTx: OpenTx, op: RowOp) {
        // GLEIF goes into the `public` schema; mirror the table name into XT.
        val table = openTx.table(TableRef(dbName, op.schema, op.table))

        when (op) {
            is RowOp.Put -> {
                val id = deriveId(op.table, op.row)
                val doc = shapeDoc(op.table, op.row) + ("_id" to id)
                val validFrom = validFromMicros(op.table, op.row) ?: openTx.systemFrom
                table.logPut(ByteBuffer.wrap(id.asIid), validFrom, Long.MAX_VALUE) {
                    table.docWriter.writeObject(doc)
                }
            }

            is RowOp.Delete -> {
                // The delete row carries only the upstream's REPLICA IDENTITY columns; deriving the id
                // needs the primary key, which the default (PK) replica identity provides.
                val id = deriveId(op.table, op.row)
                table.logDelete(ByteBuffer.wrap(id.asIid), openTx.systemFrom, Long.MAX_VALUE)
            }
        }
    }

    // GLEIF's three table families:
    //  - `lei`            the entity, _id = lei, valid from system-time (current state);
    //  - `*_changes`      state transitions, _id = lei (XT's PK is _id + _valid_from, so
    //                     successive changes are versions of one doc), valid from effective_date;
    //  - everything else  occurrences, _id = lei__effective_date (an entity can have several),
    //                     valid from effective_date.
    private fun isStateChange(table: String) = table.endsWith("_changes")
    private fun isEntity(table: String) = table == "lei"

    /** Natural key per family. */
    private fun deriveId(table: String, row: Map<String, Any?>): String = when {
        isEntity(table) || isStateChange(table) -> row.str("lei", table)
        // canonical Instant.toString() for a stable id — the column's own toString
        // (a ZonedDateTime) renders the same instant differently (e.g. drops zero
        // seconds), so two CDC views of one event could split into two identities.
        else -> "${row.str("lei", table)}__${row.instant("effective_date", table)}"
    }

    private fun validFromMicros(table: String, row: Map<String, Any?>): Long? = when {
        // entity: valid from system-time (the loader leaves the snapshot's valid-from to default).
        isEntity(table) -> null
        // state changes + occurrences: valid from when the fact became true.
        else -> row.tsMicros("effective_date")
    }

    /** Re-key PG enum columns to keyword values so the XT doc matches the loader.
     *
     * jsonb columns (addresses, other-names) arrive already parsed as maps; they come back
     * string-keyed where the loader's structs are keyword-keyed, but that distinction collapses
     * in XT — both become the same struct field — so they pass through untouched. Enum *values*
     * don't: the loader stores `:active`, and a keyword value differs from a string value in XT,
     * so those columns we re-key to keywords. */
    private fun shapeDoc(table: String, row: Map<String, Any?>): Map<String, Any?> =
        row.mapValues { (k, v) ->
            if (v is String && k in ENUM_COLS) v.kw else v
        }

    @Serializable
    @SerialName("!Gleif")
    class Factory : PgIndexer.Factory {
        override fun open(dbName: String): PgIndexer = GleifPgIndexer(dbName)
        override fun equals(other: Any?) = other is Factory
        override fun hashCode() = javaClass.hashCode()

        // Registered (META-INF/services) so the factory can travel as persisted
        // secondary-database config — letting a full, queryable node take this
        // indexer (an IngestNode could run it un-persisted, but has no pgwire).
        // No config fields, so the proto carrier is an empty Struct (à la
        // TestRerouteIndexer) — no generated message needed.
        class Registration : PgIndexer.Registration<Factory> {
            override val protoTag get() = "proto.xtdb.com/google.protobuf.Struct"
            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(Struct.getDefaultInstance(), "proto.xtdb.com")

            override fun fromProto(msg: ProtoAny): PgIndexer.Factory {
                msg.unpack(Struct::class.java)
                return Factory()
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<PgIndexer.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    private companion object {
        // columns the loader stores as keyword values (PG enum types, plus
        // validation_documents which is text in PG but a keyword on the XT side).
        val ENUM_COLS = setOf(
            "entity_status", "registration_status", "entity_category",
            "status", "group_type", "validation_documents",
        )

        fun Map<String, Any?>.str(col: String, table: String): String =
            this[col]?.toString()
                ?: throw Incorrect(
                    "missing '$col' in row from $table",
                    errorCode = "xtdb.datasets.gleif/missing-column",
                    data = mapOf("column" to col, "table" to table),
                )

        // pgjdbc/pgoutput hands timestamptz back as ZonedDateTime or OffsetDateTime; accept both.
        fun Map<String, Any?>.instant(col: String, table: String): Instant = when (val v = this[col]) {
            is ZonedDateTime -> v.toInstant()
            is OffsetDateTime -> v.toInstant()
            is Instant -> v
            else -> throw Incorrect(
                "'$col' must be a non-null TIMESTAMPTZ column",
                errorCode = "xtdb.datasets.gleif/invalid-timestamp",
                data = mapOf("column" to col, "table" to table,
                             "type" to (v?.let { it::class.qualifiedName ?: it::class.simpleName })),
            )
        }

        fun Map<String, Any?>.tsMicros(col: String): Long? =
            when (val v = this[col]) {
                null -> null
                is ZonedDateTime -> v.toInstant().asMicros
                is OffsetDateTime -> v.toInstant().asMicros
                is Instant -> v.asMicros
                else -> throw Incorrect(
                    "'$col' must be a TIMESTAMPTZ column",
                    errorCode = "xtdb.datasets.gleif/invalid-timestamp",
                    data = mapOf("column" to col, "type" to (v::class.qualifiedName ?: v::class.simpleName)),
                )
            }
    }
}
