package xtdb.query

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.error.Incorrect
import xtdb.arrow.RelationReader
import xtdb.tx.TxOp
import java.time.ZoneId

interface SqlPlanner {
    /**
     * Evaluate a standalone SQL expression (a transaction-option or SET value) to its runtime value,
     * substituting positional params (?_0, ?_1, …) from the columns of [args].
     *
     * [expr] is the parsed expression context — a `Sql.ExprContext` or `Sql.LiteralContext`.
     */
    fun evalLiteral(expr: ParserRuleContext, args: RelationReader?): Any?

    /**
     * Eager-expand a DML statement into core [TxOp]s (e.g. INSERT/PATCH RECORDS → PutDocs/PatchDocs),
     * reading positional params (?_0, ?_1, …) column-major from [args].
     *
     * Returns null when the statement isn't statically expandable — the caller submits the raw SQL op and
     * lets the indexer expand it at index time.
     */
    fun toStaticOps(sql: String, args: RelationReader?, al: BufferAllocator, defaultTz: ZoneId?): List<TxOp>?
}

/**
 * Validate that [this]'s columns name their SQL parameters acceptably, then rename them to the internal
 * positional convention (`?_0`, `?_1`, …) by ordinal position, ready to hand to the planner.
 *
 * SQL parameters bind by ordinal position. To stop a caller's evident intent being silently reordered —
 * e.g. columns named `$3, $2, $1` bound to `$1, $2, $3` by physical order — we accept only columns that
 * are already correctly ordered:
 *
 * - **unnamed** — every name empty, as a spec-compliant ADBC client supplies (positional identity by ordinal);
 * - **`$1..$N`** — the 1-indexed wire/user convention (also what `bind(VectorSchemaRoot)` normalises to);
 * - **`?_0..?_{N-1}`** — the 0-indexed internal convention, as pgwire's `open-args` and the planner emit.
 *
 * Anything else — arbitrary names, gaps, or out-of-order numbering — throws, rather than binding by position
 * and quietly ignoring the names.
 *
 * The result is a renaming view over the same vectors — closing it closes them — so it can be handed straight
 * to a cursor that takes ownership of its args (as `openQuery` does), with no copy.
 */
fun RelationReader.withPositionalParamNames(): RelationReader {
    val names = vectors.map { it.name }

    val ok = names.all { it.isEmpty() }
            || names.withIndex().all { (i, nm) -> nm == "\$${i + 1}" }
            || names.withIndex().all { (i, nm) -> nm == "?_$i" }

    if (!ok)
        // The message states the public contract only — the `?_0..` form is an internal convention
        // (pgwire/the planner) that we deliberately don't advertise to callers.
        throw Incorrect(
            "SQL parameters must be supplied unnamed, or named \$1..\$${names.size} in order; got $names",
            "xtdb/invalid-param-names",
            mapOf("names" to names),
        )

    return RelationReader.from(vectors.mapIndexed { idx, v -> v.withName("?_$idx") }, rowCount)
}
