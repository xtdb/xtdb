package xtdb.api

sealed interface TransactionResult : TransactionKey

interface TransactionCommitted : TransactionResult

interface TransactionAborted : TransactionResult {
    val error: Throwable
}
