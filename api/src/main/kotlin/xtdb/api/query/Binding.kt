package xtdb.api.query

import xtdb.api.query.Expr.Companion.lVar
import xtdb.api.query.Expr.Companion.param

data class Binding(val binding: String, val expr: Expr) {
    companion object {
        @JvmStatic
        @JvmOverloads
        fun bindVar(binding: String, bindVar: String = binding) = Binding(binding, lVar(bindVar))

        @JvmStatic
        @JvmOverloads
        fun bindParam(binding: String, bindParam: String = binding) = Binding(binding, param(bindParam))
    }
}

infix fun String.toVar(`var`: String) = Binding(this, lVar(`var`))
infix fun String.toParam(param: String) = Binding(this, param(param))
