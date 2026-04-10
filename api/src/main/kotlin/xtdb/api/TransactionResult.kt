package xtdb.api

sealed interface TransactionResult {
    val txKey: TransactionKey

    data class Committed(override val txKey: TransactionKey) : TransactionResult
    data class Aborted(override val txKey: TransactionKey, val error: Throwable?) : TransactionResult
}
