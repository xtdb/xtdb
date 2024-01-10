package xtdb.api.tx

import kotlinx.serialization.Serializable

@Serializable
data class TxRequest(val txOps: List<TxOp>, val opts: TxOptions? = null)
