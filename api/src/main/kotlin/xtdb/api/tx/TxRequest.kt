package xtdb.api.tx

import kotlinx.serialization.Serializable

/**
 * @suppress
 */
@Serializable
data class TxRequest(val txOps: List<TxOp>, val opts: TxOptions? = null)
