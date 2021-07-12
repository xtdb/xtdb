package crux.api.query.context

import clojure.lang.Keyword
import clojure.lang.Symbol
import crux.api.query.domain.QuerySection.WhereSection
import crux.api.query.domain.WhereClause
import crux.api.query.domain.WhereClause.*
import crux.api.query.domain.WhereClause.Join.Type.*
import crux.api.query.domain.WhereClause.Predicate.Type.*
import crux.api.underware.*

class WhereContext private constructor(): ComplexBuilderContext<WhereClause, WhereSection>(::WhereSection) {
    companion object: BuilderContext.Companion<WhereSection, WhereContext>(::WhereContext)

    operator fun WhereClause.unaryPlus() = add(this)

    data class SymbolAndKey(val symbol: Symbol, val key: Keyword)

    infix fun Symbol.has(key: Keyword) =
        SymbolAndKey(this, key).also {
            add(HasKey(this, key))
        }

    infix fun SymbolAndKey.eq(value: Any) = replace(HasKeyEqualTo(symbol, key, value))

    private fun join(type: Join.Type, block: WhereContext.() -> Unit) = add(Join(type, build(block)))

    fun not(block: WhereContext.() -> Unit) = join(NOT, block)
    fun or(block: WhereContext.() -> Unit) = join(OR, block)

    private fun pred(predicateType: Predicate.Type, i: Symbol, j: Any) = add(Predicate(predicateType, i, j))

    infix fun Symbol.gt(other: Any) = pred(GT, this, other)
    infix fun Symbol.lt(other: Any) = pred(LT, this, other)
    infix fun Symbol.gte(other: Any) = pred(GTE, this, other)
    infix fun Symbol.lte(other: Any) = pred(LTE, this, other)
    infix fun Symbol.eq(other: Any) = pred(EQ, this, other)
    infix fun Symbol.neq(other: Any) = pred(NEQ, this, other)

    data class RuleDeclaration(val name: Symbol)
    fun rule(name: Symbol) = RuleDeclaration(name)

    operator fun RuleDeclaration.invoke(vararg params: Any) = add(RuleInvocation(name, params.toList()))

    data class FunctionToSet(val type: SetToFunction.Type, val i: Symbol, val j: Any)
    operator fun Symbol.plus(other: Any) = FunctionToSet(SetToFunction.Type.PLUS, this, other)
    operator fun Symbol.times(other: Any) = FunctionToSet(SetToFunction.Type.TIMES, this, other)
    operator fun Symbol.div(other: Any) = FunctionToSet(SetToFunction.Type.DIVIDE, this, other)
    operator fun Symbol.minus(other: Any) = FunctionToSet(SetToFunction.Type.MINUS, this, other)
    infix fun FunctionToSet.eq(target: Symbol) = add(SetToFunction(target, type, i, j))
}