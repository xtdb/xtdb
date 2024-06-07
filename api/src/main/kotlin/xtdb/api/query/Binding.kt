package xtdb.api.query

import xtdb.api.query.Exprs.lVar

data class Binding(val binding: String, val expr: Expr) {
    @JvmOverloads
    constructor(binding: String, bindVar: String = binding) : this(binding, lVar(bindVar))
    constructor(binding: Pair<String, Expr>) : this(binding.first, binding.second)

    /**
     * @suppress
     */
    abstract class ABuilder<B : ABuilder<B, O>, O> {
        private val bindings: MutableList<Binding> = mutableListOf()

        @Suppress("UNCHECKED_CAST")
        fun setBindings(bindings: Collection<Binding>) = (this as B).apply {
            this.bindings.clear()
            this.bindings += bindings
        }

        @Suppress("UNCHECKED_CAST")
        fun bind(binding: String, expr: Expr): B = (this as B).apply { bindings += Binding(binding, expr) }

        @JvmOverloads
        fun bind(binding: String, varName: String = binding) = bind(binding, lVar(varName))

        @JvmSynthetic
        @JvmName("bindAll_str_str")
        fun bindAll(vararg bindings: Pair<String, String>) =
            this.apply { bindings.mapTo(this.bindings) { Binding(it.first, it.second) } }

        @JvmSynthetic
        fun bindAll(vararg bindings: Pair<String, Expr>) =
            this.apply { bindings.mapTo(this.bindings, ::Binding) }

        @Suppress("UNCHECKED_CAST")
        fun bindAll(vararg cols: String): B = (this as B).apply {
            cols.mapTo(bindings) { Binding(it, lVar(it)) }
        }

        protected fun getBindings(): List<Binding> = bindings

        abstract fun build(): O
    }

    class Builder : ABuilder<Builder, List<Binding>>() {
        override fun build() = getBindings()
    }
}
