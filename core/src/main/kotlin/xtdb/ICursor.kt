package xtdb

import io.micrometer.tracing.Tracer
import io.micrometer.tracing.Span
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.RelationReader
import java.lang.AutoCloseable
import java.time.Duration
import java.time.InstantSource
import java.util.*
import java.util.function.Consumer
import java.util.stream.StreamSupport.stream

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

    fun consume() = consume(SNAKE_CASE_STRING)

    fun <K> consume(keyFn: IKeyFn<K>): List<List<Map<K, *>>> =
        stream(this, false).map { it.toMaps(keyFn) }.toList()

    companion object {

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

        private class TracingCursor(
            private val inner: ICursor,
            private val tracer: Tracer,
            private val parentSpan: Span? = null
        ) : ICursor by inner {
            // Create span eagerly so children can use it as parent
            private val span: Span = run {
                val spanBuilder = if (parentSpan != null) {
                    tracer.nextSpan(parentSpan)
                } else {
                    tracer.nextSpan()
                }
                spanBuilder
                    .name("query.cursor.${inner.cursorType}")
                    .tag("cursor.type", inner.cursorType)
                    .start()
            }

            // Wrap child cursors with tracing using this cursor's span as parent
            private val tracedChildren: List<ICursor> by lazy {
                inner.childCursors.map { child ->
                    // Only wrap if the child isn't already wrapped
                    if (child is TracingCursor) child
                    else TracingCursor(child, tracer, span)
                }
            }

            override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
                return inner.tryAdvance { rel ->
                    c.accept(rel)
                }
            }

            override val childCursors: List<ICursor>
                get() = tracedChildren

            override fun close() {
                span.end()
                inner.close()
            }

            @Suppress("RedundantOverride")
            override fun forEachRemaining(action: Consumer<in RelationReader>?) = super.forEachRemaining(action)
            override fun getExactSizeIfKnown(): Long = inner.exactSizeIfKnown
            override fun hasCharacteristics(characteristics: Int): Boolean = inner.hasCharacteristics(characteristics)
            override fun getComparator(): Comparator<in RelationReader>? = inner.comparator
        }

        @JvmStatic
        fun ICursor.wrapExplainAnalyze(): ICursor = ExplainAnalyzeCursor(this)

        @JvmStatic
        @JvmOverloads
        fun ICursor.wrapTracing(tracer: Tracer, parentSpan: Span? = null): ICursor = TracingCursor(this, tracer, parentSpan)
    }
}
