package xtdb.query

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.RelationReader
import xtdb.tx.TxOp
import java.time.ZoneId

interface SqlPlanner {
    /**
     * Evaluate a standalone SQL expression (a transaction-option or SET value) to its runtime value,
     * substituting positional params (?_0, ?_1, …) from [args].
     *
     * [expr] is the parsed expression context — a `Sql.ExprContext` or `Sql.LiteralContext`.
     */
    fun evalLiteral(expr: ParserRuleContext, args: List<*>?): Any?

    /**
     * Eager-expand a DML statement into core [TxOp]s (e.g. INSERT/PATCH RECORDS → PutDocs/PatchDocs),
     * reading positional params (?_0, ?_1, …) column-major from [args].
     *
     * Returns null when the statement isn't statically expandable — the caller submits the raw SQL op and
     * lets the indexer expand it at index time.
     */
    fun toStaticOps(sql: String, args: RelationReader?, al: BufferAllocator, defaultTz: ZoneId?): List<TxOp>?
}
