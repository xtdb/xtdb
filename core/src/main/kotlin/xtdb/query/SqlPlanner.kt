package xtdb.query

import org.antlr.v4.runtime.ParserRuleContext

interface SqlPlanner {
    /**
     * Evaluate a standalone SQL expression (a transaction-option or SET value) to its runtime value,
     * substituting positional params (?_0, ?_1, …) from [args].
     *
     * [expr] is the parsed expression context — a `Sql.ExprContext` or `Sql.LiteralContext`.
     */
    fun evalLiteral(expr: ParserRuleContext, args: List<*>?): Any?
}
