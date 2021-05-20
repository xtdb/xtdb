package crux.api.tx

import crux.api.ICruxIngestAPI
import crux.api.TransactionInstant

fun ICruxIngestAPI.submitTx(block: TransactionContext.() -> Unit): TransactionInstant =
    submitTx(TransactionContext.build(block))