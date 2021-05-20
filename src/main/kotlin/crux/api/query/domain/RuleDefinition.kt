package crux.api.query.domain

import clojure.lang.Symbol
import crux.api.query.domain.QuerySection.WhereSection


data class RuleDefinition(val name: Symbol, val parameters: List<Symbol>, val body: WhereSection)