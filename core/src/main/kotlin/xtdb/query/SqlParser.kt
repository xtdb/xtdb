@file:JvmName("SqlParser")

package xtdb.query

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import xtdb.antlr.Sql
import xtdb.antlr.SqlLexer
import xtdb.error.Incorrect

private fun Recognizer<*, *>.addThrowingErrorListener() = apply {
    removeErrorListeners()
    addErrorListener(object : BaseErrorListener() {
        override fun syntaxError(
            recognizer: Recognizer<*, *>?,
            offendingSymbol: Any?,
            line: Int, charPositionInLine: Int, msg: String?, e: RecognitionException?
        ) {
            throw Incorrect(
                """
                Errors parsing SQL statement:
                  - line $line:$charPositionInLine $msg""".trimIndent(),
                "xtdb/sql-error",
                cause = e
            )
        }
    })
}

private fun <T> String.parseWithFallback(parseAction: (Sql) -> T): T {
    val lexer = SqlLexer(CharStreams.fromString(this)).also { it.addThrowingErrorListener() }
    val tokens = CommonTokenStream(lexer)
    val parser = Sql(tokens)

    // Try SLL mode first (fast speculative parsing)
    parser.removeErrorListeners()
    parser.errorHandler = BailErrorStrategy()
    parser.interpreter.predictionMode = PredictionMode.SLL

    return try {
        parseAction(parser)
    } catch (e: ParseCancellationException) {
        // SLL failed due to ambiguity, retry with full LL mode using same parser state
        tokens.seek(0)
        parser.reset()
        parser.addThrowingErrorListener()
        parser.errorHandler = DefaultErrorStrategy()
        parser.interpreter.predictionMode = PredictionMode.LL

        parseAction(parser)
    }
}

fun String.parseExpr(): Sql.ExprContext = parseWithFallback { it.expr() }

fun String.parseWhere(): Sql.SearchConditionContext = parseWithFallback { it.whereClause().searchCondition() }

// Estimated 400MB max size for each cache, given large SLT queries, observed during SLT between/10/ run.
// with an inferred value of roughly 100KB per parsed query.
private val singleQueryCache =
    Caffeine.newBuilder().maximumSize(4096)
        .build(CacheLoader<String, Sql.DirectlyExecutableStatementContext> { sql ->
            sql.parseWithFallback { it.directSqlStatement().directlyExecutableStatement() }
        })

private val multiQueryCache =
    Caffeine.newBuilder().maximumSize(4096)
        .build(CacheLoader<String, List<Sql.DirectlyExecutableStatementContext>> { sql ->
            sql.parseWithFallback { it.multiSqlStatement().directlyExecutableStatement() }
        })

fun String.parseStatement() = singleQueryCache[this]

fun String.parseMultiStatement() = multiQueryCache[this]