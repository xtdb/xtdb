@file:JvmName("SqlParser")

package xtdb.query

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import org.antlr.v4.runtime.*
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

private fun String.toAntlrParser() =
    SqlLexer(CharStreams.fromString(this))
        .also { it.addThrowingErrorListener() }
        .let { CommonTokenStream(it) }
        .let { Sql(it) }
        .also { it.addThrowingErrorListener() }

fun String.parseExpr(): Sql.ExprContext =
    toAntlrParser().expr()

fun String.parseWhere(): Sql.SearchConditionContext =
    toAntlrParser().whereClause().searchCondition()

private val singleQueryCache =
    Caffeine.newBuilder().maximumSize(4096)
        .build(CacheLoader<String, Sql.DirectlyExecutableStatementContext> { sql ->
            sql.toAntlrParser()
                .directSqlStatement()
                .directlyExecutableStatement()
        })

private val multiQueryCache =
    Caffeine.newBuilder().maximumSize(4096)
        .build(CacheLoader<String, List<Sql.DirectlyExecutableStatementContext>> { sql ->
            sql.toAntlrParser()
                .multiSqlStatement()
                .directlyExecutableStatement()
        })

fun String.parseStatement() = singleQueryCache[this]

fun String.parseMultiStatement() = multiQueryCache[this]