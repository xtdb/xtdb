package xtdb.tx

import xtdb.api.TxOptions

data class Tx(val txOps: List<TxOp>, val opts: TxOptions)
