@file:JvmName("SqlParser")

package xtdb.query

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import xtdb.antlr.Sql
import xtdb.antlr.SqlBaseVisitor
import xtdb.antlr.SqlLexer
import xtdb.error.Incorrect
import xtdb.query.ParsedStatement.*
import xtdb.util.normalForm

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

// RegularIdentifier folds to normal form (lower-case, dashes→underscores); a DELIMITED_IDENTIFIER keeps its
// case, quotes stripped — mirrors xtdb.sql/identifier-sym.
private fun identStr(ctx: Sql.IdentifierContext): String = when (ctx) {
    is Sql.DelimitedIdentifierContext -> ctx.text.let { it.substring(1, it.length - 1) }
    else -> normalForm(ctx.text)
}

// session parameter / variable names are lower-cased rather than normal-formed — mirrors pgwire's session-param-name.
private fun sessionParamName(ctx: Sql.IdentifierContext): String = when (ctx) {
    is Sql.DelimitedIdentifierContext -> ctx.text.substring(1, ctx.text.length - 1)
    else -> ctx.text
}.lowercase()

// the literal text of a characterString (COPY format, ATTACH config-yaml). Standard-string escapes are
// unwound; C-escape (`E'…'`) bodies are taken verbatim — adequate for the closed/opaque values we read here.
private fun charStringValue(ctx: Sql.CharacterStringContext): String = when (ctx) {
    is Sql.DollarStringContext -> ctx.dollarStringText()?.text ?: ""
    is Sql.CEscapesStringContext -> ctx.text.let { it.substring(2, it.length - 1) }
    else -> ctx.text.let { it.substring(1, it.length - 1).replace("''", "'") }
}

// an unrecognised (or absent) format yields null; the consumer reports the "requires a valid format" error.
private fun copyFormat(ctx: Sql.CopyOptsContext?): CopyFormat? =
    ctx?.copyOpt()
        ?.mapNotNull { (it as? Sql.CopyFormatOptionContext)?.format }
        ?.firstOrNull()
        ?.let {
            when (charStringValue(it)) {
                "transit-json" -> CopyFormat.TRANSIT_JSON
                "transit-msgpack" -> CopyFormat.TRANSIT_MSGPACK
                "arrow-file" -> CopyFormat.ARROW_FILE
                "arrow-stream" -> CopyFormat.ARROW_STREAM
                else -> null
            }
        }

private class ParsedStatementVisitor : SqlBaseVisitor<ParsedStatement>() {

    private fun txOptions(ctx: Sql.TransactionCharacteristicsContext?): TxOptions {
        var opts = TxOptions()
        ctx?.transactionMode()?.forEach { mode ->
            when (mode) {
                is Sql.ReadOnlyTransactionContext -> {
                    opts = opts.copy(accessMode = AccessMode.READ_ONLY)
                    mode.readOnlyTxOption().filterNotNull().forEach { opt ->
                        opts = when (opt) {
                            is Sql.SnapshotTokenTxOptionContext -> opts.copy(snapshotToken = opt.snapshotToken)
                            is Sql.SnapshotTimeTxOptionContext -> opts.copy(snapshotTime = opt.snapshotTime)
                            is Sql.ClockTimeTxOptionContext -> opts.copy(clockTime = opt.clockTime)
                            is Sql.AwaitTokenTxOptionContext -> opts.copy(awaitToken = opt.awaitToken)
                            is Sql.TxTzOption0Context -> opts.copy(defaultTz = opt.txTzOption().tz)
                            else -> opts
                        }
                    }
                }

                is Sql.ReadWriteTransactionContext -> {
                    opts = opts.copy(accessMode = AccessMode.READ_WRITE)
                    mode.readWriteTxOption().filterNotNull().forEach { opt ->
                        opts = when (opt) {
                            is Sql.SystemTimeTxOptionContext -> opts.copy(systemTime = opt.systemTime)
                            is Sql.AsyncTxOptionContext -> opts.copy(async = opt.async)
                            is Sql.TxTzOption1Context -> opts.copy(defaultTz = opt.txTzOption().tz)
                            is Sql.MetadataTxOptionContext -> opts.copy(userMetadata = opt.metadata)
                            else -> opts
                        }
                    }
                }
                // IsolationLevel — no-op for us
            }
        }
        return opts
    }

    override fun visitQueryExpr(ctx: Sql.QueryExprContext) = Query(ctx)

    override fun visitInsertStatement(ctx: Sql.InsertStatementContext) = Dml.Insert(ctx)
    override fun visitUpdateStatement(ctx: Sql.UpdateStatementContext) = Dml.Update(ctx)
    override fun visitDeleteStatement(ctx: Sql.DeleteStatementContext) = Dml.Delete(ctx)
    override fun visitPatchStatement(ctx: Sql.PatchStatementContext) = Dml.Patch(ctx)
    override fun visitEraseStatement(ctx: Sql.EraseStatementContext) = Dml.Erase(ctx)
    override fun visitAssertStatement(ctx: Sql.AssertStatementContext) = Dml.Assert(ctx)

    override fun visitGrantRoleStatement(ctx: Sql.GrantRoleStatementContext) =
        Dml.GrantRole(ctx, identStr(ctx.userName), identStr(ctx.roleName))

    override fun visitRevokeRoleStatement(ctx: Sql.RevokeRoleStatementContext) =
        Dml.RevokeRole(ctx, identStr(ctx.userName), identStr(ctx.roleName))

    override fun visitCreateTableStatement(ctx: Sql.CreateTableStatementContext): ParsedStatement {
        val tt = ctx.targetTable()
        val cols = ctx.columnNameList()?.columnName()?.map { identStr(it.identifier()) } ?: emptyList()
        return Dml.CreateTable(ctx, tt.schemaName?.let(::identStr), identStr(tt.tableName), cols)
    }

    override fun visitStartTransactionStatement(ctx: Sql.StartTransactionStatementContext) =
        Begin(ctx, txOptions(ctx.transactionCharacteristics()))

    override fun visitSetTransactionStatement(ctx: Sql.SetTransactionStatementContext) = SetTransaction(ctx)
    override fun visitCommitStatement(ctx: Sql.CommitStatementContext) =
        Commit(ctx, when {
            ctx.SYNC() != null -> CommitMode.SYNC
            ctx.ASYNC() != null -> CommitMode.ASYNC
            else -> null
        })
    override fun visitRollbackStatement(ctx: Sql.RollbackStatementContext) = Rollback(ctx)

    override fun visitSetSessionCharacteristicsStatement(ctx: Sql.SetSessionCharacteristicsStatementContext): ParsedStatement {
        var mode: AccessMode? = null
        ctx.sessionCharacteristic().forEach { ch ->
            (ch as? Sql.SessionTxCharacteristicsContext)?.sessionTxMode()?.forEach { m ->
                when (m) {
                    is Sql.ReadOnlySessionContext -> mode = AccessMode.READ_ONLY
                    is Sql.ReadWriteSessionContext -> mode = AccessMode.READ_WRITE
                    else -> {}
                }
            }
        }
        return SetSessionCharacteristics(ctx, mode)
    }

    override fun visitSetRoleStatement(ctx: Sql.SetRoleStatementContext) = SetRole(ctx)
    override fun visitSetTimeZoneStatement(ctx: Sql.SetTimeZoneStatementContext) = SetTimeZone(ctx, ctx.zone)
    override fun visitSetAwaitTokenStatement(ctx: Sql.SetAwaitTokenStatementContext) = SetAwaitToken(ctx, ctx.awaitToken)

    override fun visitSetSessionVariableStatement(ctx: Sql.SetSessionVariableStatementContext) =
        SetSessionParameter(ctx, sessionParamName(ctx.identifier()), ctx.literal())

    override fun visitShowVariableStatement(ctx: Sql.ShowVariableStatementContext) = Query(ctx)
    override fun visitShowSnapshotTokenStatement(ctx: Sql.ShowSnapshotTokenStatementContext) = Query(ctx)
    override fun visitShowClockTimeStatement(ctx: Sql.ShowClockTimeStatementContext) = Query(ctx)

    override fun visitShowSessionVariableStatement(ctx: Sql.ShowSessionVariableStatementContext) =
        ShowVariable(ctx, sessionParamName(ctx.identifier()))

    override fun visitShowAwaitTokenStatement(ctx: Sql.ShowAwaitTokenStatementContext) =
        ShowVariable(ctx, "await_token")

    override fun visitCopyInStmt(ctx: Sql.CopyInStmtContext): ParsedStatement {
        val tt = ctx.targetTable()
        return CopyIn(ctx, tt.schemaName?.let(::identStr), identStr(tt.tableName), copyFormat(ctx.opts))
    }

    override fun visitPrepareStatement(ctx: Sql.PrepareStatementContext) =
        Prepare(ctx, identStr(ctx.statementName), ctx.directlyExecutableStatement().accept(this))

    override fun visitExecuteStatement(ctx: Sql.ExecuteStatementContext) =
        Execute(ctx, identStr(ctx.statementName))

    override fun visitAttachDatabaseStatement(ctx: Sql.AttachDatabaseStatementContext) =
        AttachDatabase(ctx, identStr(ctx.dbName), ctx.configYaml?.let(::charStringValue))

    override fun visitDetachDatabaseStatement(ctx: Sql.DetachDatabaseStatementContext) =
        DetachDatabase(ctx, identStr(ctx.dbName))
}

/**
 * Classifies a (possibly multi-statement) SQL string into [ParsedStatement]s without planning.
 * Empty/comment-only input and pgwire's Postgres-emulation rewrites (canned responses, query replacement)
 * are the frontend's concern and are not handled here.
 */
fun parseStatements(sql: String): List<ParsedStatement> =
    ParsedStatementVisitor().let { v -> sql.parseMultiStatement().map { it.accept(v) } }