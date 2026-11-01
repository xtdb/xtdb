package xtdb.util

import java.util.concurrent.locks.StampedLock
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

@OptIn(ExperimentalContracts::class)
internal inline fun <R> StampedLock.withReadLock(block: () -> R): R {
    contract { callsInPlace(block, InvocationKind.EXACTLY_ONCE) }
    val stamp = readLock()
    try {
        return block()
    } finally {
        unlock(stamp)
    }
}

@OptIn(ExperimentalContracts::class)
internal inline fun <R> StampedLock.withWriteLock(block: () -> R): R {
    contract { callsInPlace(block, InvocationKind.EXACTLY_ONCE) }
    val stamp = writeLock()
    try {
        return block()
    } finally {
        unlock(stamp)
    }
}
