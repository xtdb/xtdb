package xtdb.query

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.arrow.memory.BufferAllocator
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
 * Rename [this]'s columns to the positional SQL-parameter convention (`?_0`, `?_1`, …) by ordinal position,
 * discarding whatever the caller named them: SQL parameters are matched by position, not name.
 *
 * The result is a renaming view over the same vectors — closing it closes them — so it can be handed straight
 * to a cursor that takes ownership of its args (as `openQuery` does), with no copy.
 */
fun RelationReader.withPositionalParamNames(): RelationReader =
    RelationReader.from(vectors.mapIndexed { idx, v -> v.withName("?_$idx") }, rowCount)
