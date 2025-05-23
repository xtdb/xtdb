package xtdb

import java.lang.AutoCloseable
import java.util.*

interface ICursor<E> : Spliterator<E>, AutoCloseable {
    override fun trySplit(): Spliterator<E>? = null
    override fun characteristics() = Spliterator.IMMUTABLE
    override fun estimateSize(): Long = Long.MAX_VALUE

    override fun close() = Unit
}
