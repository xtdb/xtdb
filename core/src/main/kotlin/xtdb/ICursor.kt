package xtdb

import io.micrometer.tracing.Tracer
import io.micrometer.tracing.Span
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.query.IKeyFn
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.RelationReader
import xtdb.time.InstantUtil.asMicros
import java.lang.AutoCloseable
import java.time.Duration
import java.time.Instant
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
        val pageCount: Int
        val timeToFirstPage: Duration?
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
            override var pageCount: Int = 0; private set
            override var rowCount: Long = 0; private set

            override var timeToFirstPage: Duration? = null; private set
            override var totalTime: Duration = Duration.ZERO; private set

            override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
                val pageStart = clock.instant()

                return inner.tryAdvance { rel ->
                    val pageTime = Duration.between(pageStart, clock.instant())

                    timeToFirstPage = timeToFirstPage ?: pageTime

                    rowCount += rel.rowCount
                    pageCount++

                    c.accept(rel)
                }.also {
                    totalTime += Duration.between(pageStart, clock.instant())
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
            private val parentSpan: Span,
            private val clock: InstantSource = InstantSource.system()
        ) : ICursor by inner {
            private var pageCount: Int = 0;
            private var rowCount: Long = 0;
            private var started = false
            private var startTime = clock.instant()
            private var timeToFirstPage: Duration? = null
            private var totalTime: Duration = Duration.ZERO
            private var span: Span? = null

            override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
                if (!started) {
                    started = true
                    startTime = clock.instant()
                    span = tracer.nextSpan(parentSpan)!!
                        .name("query.cursor.${inner.cursorType}")
                        .tag("cursor.type", inner.cursorType)
                        .start()
                }
                val pageStart = clock.instant()

                return inner.tryAdvance { rel ->
                    val pageTime = Duration.between(pageStart, clock.instant())
                    timeToFirstPage = timeToFirstPage ?: pageTime
                    rowCount += rel.rowCount
                    pageCount++
                    c.accept(rel)
                }.also {
                    totalTime += Duration.between(pageStart, clock.instant())
                }
            }

            override fun close() {
                span!!.tag("cursor.total_time_ms", totalTime.toMillis().toString())
                timeToFirstPage?.let { ttfp ->
                    span!!.tag("cursor.time_to_first_page_ms", ttfp.toMillis().toString())
                }
                span!!.tag("cursor.page_count", pageCount.toString())
                span!!.tag("cursor.row_count", rowCount.toString())
                inner.close()
                val endTime = startTime.plus(totalTime)
                span!!.end(endTime.asMicros, java.util.concurrent.TimeUnit.MICROSECONDS)
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
        fun ICursor.wrapTracing(tracer: Tracer, span: Span): ICursor = TracingCursor(this, tracer, span)
    }
}
