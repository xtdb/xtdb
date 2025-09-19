package xtdb.api

import xtdb.util.requiringResolve
import java.time.Instant

sealed interface TransactionResult : TransactionKey

interface TransactionCommitted : TransactionResult {
    companion object {
        operator fun invoke(txId: Long, systemTime: Instant) =
            requiringResolve("xtdb.serde/->tx-committed").invoke(txId, systemTime) as TransactionCommitted
    }
}

interface TransactionAborted : TransactionResult {
    val error: Throwable

    companion object {
        operator fun invoke(txId: Long, systemTime: Instant, error: Throwable) =
            requiringResolve("xtdb.serde/->tx-aborted").invoke(txId, systemTime, error) as TransactionAborted
    }
}