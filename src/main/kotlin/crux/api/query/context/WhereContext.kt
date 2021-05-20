package crux.api.query.context

import clojure.lang.Keyword
import clojure.lang.Symbol
import crux.api.query.domain.QuerySection.WhereSection
import crux.api.query.domain.WhereClause
import crux.api.query.domain.WhereClause.Join
import crux.api.query.domain.WhereClause.Join.Type.*
import crux.api.query.domain.WhereClause.Predicate
import crux.api.query.domain.WhereClause.Predicate.Type.*
import crux.api.underware.*

class WhereContext private constructor(): BuilderContext<WhereSection> {
    companion object {
        fun build(block: WhereContext.() -> Unit) = WhereContext().also(block).build()
    }

    private val clauses = mutableListOf<WhereClause>()

    private var hangingClause: WhereClause? = null

    private fun add(clause: WhereClause) {
        clauses.add(clause)
    }

    operator fun WhereClause.unaryPlus() = add(this)

    data class SymbolAndKey(val symbol: Symbol, val key: Keyword)

    infix fun Symbol.has(key: Keyword) =
        SymbolAndKey(this, key).also {
            lockIn()
            hangingClause = WhereClause.HasKey(this, key)
        }

    infix fun SymbolAndKey.eq(value: Any) {
        hangingClause = WhereClause.HasKeyEqualTo(symbol, key, value)
    }

    private fun join(type: Join.Type, block: WhereContext.() -> Unit) {
        lockIn()
        hangingClause = Join(type, build(block))
    }

    fun not(block: WhereContext.() -> Unit) = join(NOT, block)
    fun or(block: WhereContext.() -> Unit) = join(OR, block)

    private fun pred(predicateType: Predicate.Type, i: Symbol, j: Any) {
        lockIn()
        hangingClause = Predicate(predicateType, i, j)
    }

    infix fun Symbol.gt(other: Any) = pred(GT, this, other)
    infix fun Symbol.lt(other: Any) = pred(LT, this, other)
    infix fun Symbol.gte(other: Any) = pred(GTE, this, other)
    infix fun Symbol.lte(other: Any) = pred(LTE, this, other)
    infix fun Symbol.eq(other: Any) = pred(EQ, this, other)
    infix fun Symbol.neq(other: Any) = pred(NEQ, this, other)

    data class RuleInvocation(val name: Symbol)
    fun rule(name: Symbol) = RuleInvocation(name).also { lockIn() }

    operator fun RuleInvocation.invoke(vararg params: Any) {
        hangingClause = WhereClause.RuleInvocation(name, params.toList())
    }

    private fun lockIn() {
        hangingClause?.run(clauses::add)
        hangingClause = null
    }

    override fun build(): WhereSection {
        lockIn()
        return WhereSection(clauses)
    }
}