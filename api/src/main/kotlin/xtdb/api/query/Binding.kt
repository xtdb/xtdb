package xtdb.api.query

import xtdb.api.query.Expr.Companion.lVar

data class Binding(val binding: String, val expr: Expr) {
    @JvmOverloads
    constructor(binding: String, bindVar: String = binding) : this(binding, lVar(bindVar))

    companion object {
        @JvmStatic
        fun cols(vararg cols: String) = cols.map(::Binding).toTypedArray()
    }

    abstract class ABuilder<B : ABuilder<B, O>, O> {
        private val bindings: MutableList<Binding> = mutableListOf()

        fun setBindings(bindings: Collection<Binding>) = this.apply {
            this.bindings.clear()
            this.bindings.addAll(bindings)
        }

        @Suppress("UNCHECKED_CAST")
        fun bind(binding: String, expr: Expr): B = this.apply { bind(Binding(binding, expr)) } as B

        @Suppress("UNCHECKED_CAST")
        fun bind(binding: Binding): B = this.apply { bindings += binding } as B

        @JvmSynthetic
        infix fun String.boundTo(expr: Expr) = bind(this, expr)

        @JvmSynthetic
        infix fun String.boundTo(varName: String) = bind(this, lVar(varName))

        @JvmSynthetic
        operator fun String.unaryPlus() = this boundTo this

        @JvmSynthetic
        @JvmName("bindCols")
        operator fun Collection<String>.unaryPlus() = this.apply { +map { Binding(it, lVar(it)) } }

        @JvmSynthetic
        @JvmName("bind")
        operator fun Collection<Binding>.unaryPlus() = this.apply { bindings += this }

        @JvmSynthetic
        operator fun Binding.unaryPlus() = this.apply { bindings += this }

        @Suppress("UNCHECKED_CAST")
        fun bindCols(vararg cols: String): B = this.apply { +cols.toList() } as B

        protected fun buildBindings(): List<Binding> = bindings

        abstract fun build(): O
    }

    class Builder : ABuilder<Builder, List<Binding>>() {
        override fun build() = buildBindings()
    }
}
