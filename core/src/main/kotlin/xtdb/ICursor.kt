package xtdb

import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.RelationReader
import java.lang.AutoCloseable
import java.time.Duration
import java.time.InstantSource
import java.util.*
import java.util.function.Consumer
import java.util.stream.StreamSupport

interface ICursor : Spliterator<RelationReader>, AutoCloseable {
    interface Factory {
        fun open(): ICursor
    }

    sealed interface ExplainAnalyze {
        val rowCount: Long
        val blockCount: Int
        val timeToFirstBlock: Duration?
        val totalTime: Duration
    }

    val cursorType: String
    val childCursors: List<ICursor>
    val explainAnalyze: ExplainAnalyze? get() = null

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

        private class ExplainAnalyzeCursor(
            private val inner: ICursor,
            private val clock: InstantSource = InstantSource.system()
        ) : ICursor by inner, ExplainAnalyze {
            override var blockCount: Int = 0; private set
            override var rowCount: Long = 0; private set

            override var timeToFirstBlock: Duration? = null; private set
            override var totalTime: Duration = Duration.ZERO; private set

            override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
                val blockStart = clock.instant()

                return inner.tryAdvance { rel ->
                    val blockTime = Duration.between(blockStart, clock.instant())

                    timeToFirstBlock = timeToFirstBlock ?: blockTime

                    rowCount += rel.rowCount
                    blockCount++

                    c.accept(rel)
                }.also {
                    totalTime += Duration.between(blockStart, clock.instant())
                }
            }

            override val explainAnalyze get() = this

            @Suppress("RedundantOverride") // otherwise `ICursor by inner` also complains
            override fun forEachRemaining(action: Consumer<in RelationReader>?) = super.forEachRemaining(action)
            override fun getExactSizeIfKnown(): Long = inner.exactSizeIfKnown
            override fun hasCharacteristics(characteristics: Int): Boolean = inner.hasCharacteristics(characteristics)
            override fun getComparator(): Comparator<in RelationReader>? = inner.comparator
        }

        @JvmStatic
        fun ICursor.wrapExplainAnalyze(): ICursor = ExplainAnalyzeCursor(this)
    }
}
