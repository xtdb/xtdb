package xtdb.api.tx

import xtdb.api.IXtdbSubmitClient
import xtdb.api.TransactionInstant

fun IXtdbSubmitClient.submitTx(block: TransactionContext.() -> Unit): TransactionInstant =
    submitTx(TransactionContext.build(block))