package xtdb

import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.RelationReader
import java.lang.AutoCloseable
import java.util.*
import java.util.function.Consumer
import java.util.stream.StreamSupport

interface ICursor : Spliterator<RelationReader>, AutoCloseable {
    interface Factory {
        fun open(): ICursor
    }

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean
    override fun trySplit(): Spliterator<RelationReader>? = null
    override fun characteristics() = Spliterator.IMMUTABLE
    override fun estimateSize(): Long = Long.MAX_VALUE

    override fun close() = Unit

    companion object {
        @JvmStatic
        fun ICursor.toMaps(): List<Map<*, *>> =
            StreamSupport.stream(this, false).flatMap { it.toMaps(SNAKE_CASE_STRING).stream() }.toList()

        @JvmStatic
        fun <K> ICursor.toMaps(keyFn: IKeyFn<K>): List<Map<K, *>> =
            StreamSupport.stream(this, false).flatMap { it.toMaps(keyFn).stream() }.toList()
    }
}
