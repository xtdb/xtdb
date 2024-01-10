package xtdb.api.tx

data class TxRequest(val txOps: List<TxOp>, val opts: TxOptions)
