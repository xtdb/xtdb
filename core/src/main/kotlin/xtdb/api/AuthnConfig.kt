package xtdb.api

import kotlinx.serialization.Serializable
import xtdb.api.AuthnConfig.*

private val DEFAULT_RULES = listOf(Rule(null, null, Method.TRUST))

@Serializable
data class AuthnConfig(val rules: List<Rule> = DEFAULT_RULES) {

    @Serializable
    enum class Method {
        TRUST,
        PASSWORD,
    }

    @Serializable
    data class Rule(val user: String?, val address: String?, val method: Method)
}
