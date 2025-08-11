package xtdb

import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.RelationReader
import java.lang.AutoCloseable
import java.util.*
import java.util.function.Consumer
import java.util.stream.StreamSupport

interface ICursor<E> : Spliterator<E>, AutoCloseable {
    override fun tryAdvance(c: Consumer<in E>): Boolean
    override fun trySplit(): Spliterator<E>? = null
    override fun characteristics() = Spliterator.IMMUTABLE
    override fun estimateSize(): Long = Long.MAX_VALUE

    override fun close() = Unit

    companion object {
        @JvmStatic
        fun ICursor<RelationReader>.toMaps(): List<Map<*, *>> =
            StreamSupport.stream(this, false).flatMap { it.toMaps(SNAKE_CASE_STRING).stream() }.toList()

        @JvmStatic
        fun <K> ICursor<RelationReader>.toMaps(keyFn: IKeyFn<K>): List<Map<K, *>> =
            StreamSupport.stream(this, false).flatMap { it.toMaps(keyFn).stream() }.toList()
    }
}
