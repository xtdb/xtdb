package xtdb.api.query.domain

import clojure.lang.Symbol
import xtdb.api.query.domain.QuerySection.WhereSection


data class RuleDefinition(val name: Symbol, val boundParameters: List<Symbol>, val parameters: List<Symbol>, val body: WhereSection)