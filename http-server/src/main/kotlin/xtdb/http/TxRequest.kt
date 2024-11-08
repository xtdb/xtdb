package xtdb.http

import kotlinx.serialization.Serializable
import xtdb.api.tx.TxOp

/**
 * @suppress
 */
@Serializable
data class TxRequest(val txOps: List<TxOp.Sql>, val opts: TxOptions? = null)
