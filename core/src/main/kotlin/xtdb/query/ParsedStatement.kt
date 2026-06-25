package xtdb.query

import org.antlr.v4.runtime.misc.Interval
import xtdb.antlr.Sql

/**
 * A directly-executable SQL statement, classified from the parse tree without planning.
 *
 * This is the frontend-agnostic result of [parseStatements] — pgwire (and, in time, ADBC/Flight SQL) dispatch
 * on it rather than re-walking the grammar. Every statement carries its [ast]; [originalSql] derives from it.
 * User-supplied values are kept as the raw [Sql.ExprContext] (e.g. [SetTimeZone]) since those are grammar
 * expressions, not literals — planning and arg-application stay with the consumer.
 */
sealed interface ParsedStatement {

    /** The parse-tree node for this statement (a labelled alternative of `directlyExecutableStatement`). */
    val ast: Sql.DirectlyExecutableStatementContext

    /** This statement's original source text — the exact slice of the (possibly multi-statement) input. */
    val originalSql: String
        get() = ast.start.inputStream.getText(Interval.of(ast.start.startIndex, ast.stop.stopIndex))

    enum class AccessMode { READ_ONLY, READ_WRITE }

    enum class CopyFormat { TRANSIT_JSON, TRANSIT_MSGPACK, ARROW_FILE, ARROW_STREAM }

    /** Begin-time transaction characteristics. Expression-typed options are carried un-planned. */
    data class TxOptions(
        val accessMode: AccessMode? = null,
        val systemTime: Sql.ExprContext? = null,
        val snapshotToken: Sql.ExprContext? = null,
        val snapshotTime: Sql.ExprContext? = null,
        val clockTime: Sql.ExprContext? = null,
        val awaitToken: Sql.ExprContext? = null,
        val defaultTz: Sql.ExprContext? = null,
        val userMetadata: Sql.ExprContext? = null,
        val async: Sql.LiteralContext? = null,
    )

    data class Query(override val ast: Sql.DirectlyExecutableStatementContext) : ParsedStatement

    /** DML and DDL that submit as transaction operations; [originalSql] is the submittable statement text. */
    sealed interface Dml : ParsedStatement {
        data class Insert(override val ast: Sql.DirectlyExecutableStatementContext) : Dml
        data class Update(override val ast: Sql.DirectlyExecutableStatementContext) : Dml
        data class Delete(override val ast: Sql.DirectlyExecutableStatementContext) : Dml
        data class Patch(override val ast: Sql.DirectlyExecutableStatementContext) : Dml
        data class Erase(override val ast: Sql.DirectlyExecutableStatementContext) : Dml
        data class Assert(override val ast: Sql.DirectlyExecutableStatementContext) : Dml

        data class GrantRole(override val ast: Sql.DirectlyExecutableStatementContext, val user: String, val role: String) : Dml
        data class RevokeRole(override val ast: Sql.DirectlyExecutableStatementContext, val user: String, val role: String) : Dml

        // schema/table are the parsed name parts; resolving to a TableRef needs the connection's default-db.
        data class CreateTable(
            override val ast: Sql.DirectlyExecutableStatementContext,
            val schema: String?, val table: String, val colNames: List<String>
        ) : Dml
    }

    data class Begin(override val ast: Sql.DirectlyExecutableStatementContext, val txOptions: TxOptions) : ParsedStatement
    data class Commit(override val ast: Sql.DirectlyExecutableStatementContext) : ParsedStatement
    data class Rollback(override val ast: Sql.DirectlyExecutableStatementContext) : ParsedStatement

    /** SET TRANSACTION ISOLATION LEVEL — a no-op for us, kept so the protocol acknowledges it. */
    data class SetTransaction(override val ast: Sql.DirectlyExecutableStatementContext) : ParsedStatement

    data class SetSessionCharacteristics(override val ast: Sql.DirectlyExecutableStatementContext, val accessMode: AccessMode?) : ParsedStatement

    data class SetTimeZone(override val ast: Sql.DirectlyExecutableStatementContext, val zone: Sql.ExprContext) : ParsedStatement
    data class SetAwaitToken(override val ast: Sql.DirectlyExecutableStatementContext, val token: Sql.ExprContext) : ParsedStatement
    data class SetSessionParameter(override val ast: Sql.DirectlyExecutableStatementContext, val name: String, val value: Sql.LiteralContext) : ParsedStatement
    data class SetRole(override val ast: Sql.DirectlyExecutableStatementContext) : ParsedStatement

    /** SHOW variants answered from session state rather than as a SQL query. */
    data class ShowVariable(override val ast: Sql.DirectlyExecutableStatementContext, val variable: String) : ParsedStatement

    data class CopyIn(
        override val ast: Sql.DirectlyExecutableStatementContext,
        val schema: String?, val table: String, val format: CopyFormat?
    ) : ParsedStatement

    data class Prepare(override val ast: Sql.DirectlyExecutableStatementContext, val name: String, val inner: ParsedStatement) : ParsedStatement
    data class Execute(override val ast: Sql.DirectlyExecutableStatementContext, val name: String) : ParsedStatement

    data class AttachDatabase(override val ast: Sql.DirectlyExecutableStatementContext, val dbName: String, val configYaml: String?) : ParsedStatement
    data class DetachDatabase(override val ast: Sql.DirectlyExecutableStatementContext, val dbName: String) : ParsedStatement

    /**
     * Double-dispatch over the statement variants.
     *
     * Exists for ergonomic consumption from Clojure (pgwire): a `reify` of [Visitor] overrides only the variants
     * it cares about — the rest fall through the default chain (specific → [Visitor.visitDml] → [Visitor.visitOther]),
     * and [Visitor.visitOther] throws unless overridden, a loud guard for genuinely-unexpected statements. Each
     * method receives the typed variant, so the call site reads fields without instance?-dispatch or reflection
     * hints. Kotlin consumers should `when` over the sealed type instead. This (and [Visitor]) can be removed
     * if/when the Clojure consumers migrate to Kotlin.
     */
    fun <R> accept(visitor: Visitor<R>): R = when (this) {
        is Query -> visitor.visitQuery(this)
        is Dml.Insert -> visitor.visitInsert(this)
        is Dml.Update -> visitor.visitUpdate(this)
        is Dml.Delete -> visitor.visitDelete(this)
        is Dml.Patch -> visitor.visitPatch(this)
        is Dml.Erase -> visitor.visitErase(this)
        is Dml.Assert -> visitor.visitAssert(this)
        is Dml.GrantRole -> visitor.visitGrantRole(this)
        is Dml.RevokeRole -> visitor.visitRevokeRole(this)
        is Dml.CreateTable -> visitor.visitCreateTable(this)
        is Begin -> visitor.visitBegin(this)
        is Commit -> visitor.visitCommit(this)
        is Rollback -> visitor.visitRollback(this)
        is SetTransaction -> visitor.visitSetTransaction(this)
        is SetSessionCharacteristics -> visitor.visitSetSessionCharacteristics(this)
        is SetTimeZone -> visitor.visitSetTimeZone(this)
        is SetAwaitToken -> visitor.visitSetAwaitToken(this)
        is SetSessionParameter -> visitor.visitSetSessionParameter(this)
        is SetRole -> visitor.visitSetRole(this)
        is ShowVariable -> visitor.visitShowVariable(this)
        is CopyIn -> visitor.visitCopyIn(this)
        is Prepare -> visitor.visitPrepare(this)
        is Execute -> visitor.visitExecute(this)
        is AttachDatabase -> visitor.visitAttachDatabase(this)
        is DetachDatabase -> visitor.visitDetachDatabase(this)
    }

    interface Visitor<R> {
        fun visitOther(stmt: ParsedStatement): R =
            throw UnsupportedOperationException("unhandled statement: ${stmt::class.simpleName}")

        fun visitQuery(stmt: Query): R = visitOther(stmt)

        fun visitDml(stmt: Dml): R = visitOther(stmt)
        fun visitInsert(stmt: Dml.Insert): R = visitDml(stmt)
        fun visitUpdate(stmt: Dml.Update): R = visitDml(stmt)
        fun visitDelete(stmt: Dml.Delete): R = visitDml(stmt)
        fun visitPatch(stmt: Dml.Patch): R = visitDml(stmt)
        fun visitErase(stmt: Dml.Erase): R = visitDml(stmt)
        fun visitAssert(stmt: Dml.Assert): R = visitDml(stmt)
        fun visitGrantRole(stmt: Dml.GrantRole): R = visitDml(stmt)
        fun visitRevokeRole(stmt: Dml.RevokeRole): R = visitDml(stmt)
        fun visitCreateTable(stmt: Dml.CreateTable): R = visitDml(stmt)

        fun visitBegin(stmt: Begin): R = visitOther(stmt)
        fun visitCommit(stmt: Commit): R = visitOther(stmt)
        fun visitRollback(stmt: Rollback): R = visitOther(stmt)
        fun visitSetTransaction(stmt: SetTransaction): R = visitOther(stmt)
        fun visitSetSessionCharacteristics(stmt: SetSessionCharacteristics): R = visitOther(stmt)
        fun visitSetTimeZone(stmt: SetTimeZone): R = visitOther(stmt)
        fun visitSetAwaitToken(stmt: SetAwaitToken): R = visitOther(stmt)
        fun visitSetSessionParameter(stmt: SetSessionParameter): R = visitOther(stmt)
        fun visitSetRole(stmt: SetRole): R = visitOther(stmt)
        fun visitShowVariable(stmt: ShowVariable): R = visitOther(stmt)
        fun visitCopyIn(stmt: CopyIn): R = visitOther(stmt)
        fun visitPrepare(stmt: Prepare): R = visitOther(stmt)
        fun visitExecute(stmt: Execute): R = visitOther(stmt)
        fun visitAttachDatabase(stmt: AttachDatabase): R = visitOther(stmt)
        fun visitDetachDatabase(stmt: DetachDatabase): R = visitOther(stmt)
    }
}
