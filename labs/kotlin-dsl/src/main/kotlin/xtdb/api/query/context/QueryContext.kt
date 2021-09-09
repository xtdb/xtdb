package xtdb.api.query.context

import clojure.lang.Symbol
import xtdb.api.query.domain.Query
import xtdb.api.query.domain.QuerySection
import xtdb.api.query.domain.QuerySection.LimitSection
import xtdb.api.query.domain.QuerySection.OffsetSection
import xtdb.api.underware.BuilderContext
import xtdb.api.underware.SimpleBuilderContext

sealed class QueryContextBase: SimpleBuilderContext<QuerySection, Query>(::Query) {
    operator fun QuerySection.unaryPlus() = add(this)
    fun where(block: WhereContext.() -> Unit) = +WhereContext.build(block)
    fun order(block: OrderContext.() -> Unit) = +OrderContext.build(block)
    fun rules(block: RulesContext.() -> Unit) = +RulesContext.build(block)
    fun bind(block: BindContext.() -> Unit) = +BindContext.build(block)

    var offset: Int
        get() = throw UnsupportedOperationException()
        set(value) {
            +OffsetSection(value)
        }

    var limit: Int
        get() = throw UnsupportedOperationException()
        set(value) {
            +LimitSection(value)
        }
}

class QueryContext private constructor(): QueryContextBase()  {
    companion object: BuilderContext.Companion<Query, QueryContext>(::QueryContext)

    fun find(block: FindContext.() -> Unit) = +FindContext.build(block)
}

class Query1Context<T> private constructor(): QueryContextBase() {
    fun find(symbol: Symbol) = + FindContext.build {
        pullAll(symbol)
    }

    companion object {
        fun <T> build(block: Query1Context<T>.() -> Unit) = Query1Context<T>().also(block).build()
    }
}

class Query2Context<T1, T2> private constructor(): QueryContextBase() {
    fun find(s1: Symbol, s2: Symbol) = + FindContext.build {
        pullAll(s1)
        pullAll(s2)
    }

    companion object {
        fun <T1, T2> build(block: Query2Context<T1, T2>.() -> Unit) = Query2Context<T1, T2>().also(block).build()
    }
}

class Query3Context<T1, T2, T3> private constructor(): QueryContextBase() {

    fun find(s1: Symbol, s2: Symbol, s3: Symbol) = + FindContext.build {
        pullAll(s1)
        pullAll(s2)
        pullAll(s3)
    }

    companion object {
        fun <T1, T2, T3> build(block: Query3Context<T1, T2, T3>.() -> Unit) = Query3Context<T1, T2, T3>().also(block).build()
    }
}