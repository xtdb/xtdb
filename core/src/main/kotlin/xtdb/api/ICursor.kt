package xtdb.api

import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer
import xtdb.api.query.IKeyFn
import xtdb.arrow.RelationReader
import xtdb.query.ExplainAnalyze
import xtdb.time.InstantUtil.asMicros
import java.lang.AutoCloseable
import java.time.Duration
import java.time.InstantSource
import java.util.Comparator
import java.util.Spliterator
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.stream.StreamSupport

interface ICursor : Spliterator<RelationReader>, AutoCloseable {
    /** @suppress */
    interface Factory {
        fun open(): ICursor
    }

    /** @suppress */
    val cursorType: String

    /** @suppress */
    val childCursors: List<ICursor>
    val explainAnalyze: ExplainAnalyze? get() = null

    /** Operator-specific explain-analyze values, evaluated when read (after consumption), so they can include runtime counters. */
    val cursorAttributes: ExplainAnalyze.ScanAttributes? get() = null

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean
    override fun trySplit(): Spliterator<RelationReader>? = null
    override fun characteristics() = Spliterator.IMMUTABLE
    override fun estimateSize(): Long = Long.MAX_VALUE

    override fun close() = Unit

    fun consume() = consume(IKeyFn.KeyFn.SNAKE_CASE_STRING)

    fun <K> consume(keyFn: IKeyFn<K>): List<List<Map<K, *>>> =
        StreamSupport.stream(this, false).map { it.toMaps(keyFn) }.toList()

    /**
     * @suppress
     */
    companion object {

        private class TracingCursor(
            private val inner: ICursor,
            private val tracer: Tracer?,
            private val parentSpan: Span?,
            private val spanName: String? = null,
            override val pushdowns: Map<String, Any>? = null,
            private val clock: InstantSource = InstantSource.system()
        ) : ICursor by inner, ExplainAnalyze {
            override var pageCount: Int = 0; private set
            override var rowCount: Long = 0; private set
            private var started = false
            private var startTime = clock.instant()
            private var endTime = clock.instant()
            override var timeToFirstPage: Duration? = null; private set
            override var totalTime: Duration = Duration.ZERO; private set
            private var span: Span? = null

            override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
                if (!started) {
                    started = true
                    startTime = clock.instant()
                    if (tracer != null && parentSpan != null) {
                        span = tracer.nextSpan(parentSpan)
                            ?.name(spanName ?: "query.cursor.${inner.cursorType}")
                            ?.tag("cursor.type", inner.cursorType)
                            ?.start()
                    }
                }
                val pageStart = clock.instant()
                var acceptTime = Duration.ZERO

                return inner.tryAdvance { rel ->
                    val acceptStart = clock.instant()
                    timeToFirstPage = timeToFirstPage ?: Duration.between(pageStart, acceptStart)
                    rowCount += rel.rowCount
                    pageCount++
                    c.accept(rel)
                    acceptTime += Duration.between(acceptStart, clock.instant())
                }.also {
                    val pageEnd = clock.instant()
                    totalTime += Duration.between(pageStart, pageEnd) - acceptTime
                    endTime = pageEnd
                }
            }

            override fun close() {
                // read before closing the inner cursor — attributes may be derived from its (now-final) state
                val attrs = inner.cursorAttributes
                inner.close()
                span?.let { s ->
                    s.tag("cursor.total_time_ms", totalTime.toMillis().toString())
                    timeToFirstPage?.let { ttfp ->
                        s.tag("cursor.time_to_first_page_ms", ttfp.toMillis().toString())
                    }
                    s.tag("cursor.page_count", pageCount.toString())
                    s.tag("cursor.row_count", rowCount.toString())
                    attrs?.toMap()?.forEach { (k, v) -> s.tag(k, v.toString()) }
                    s.end(endTime.asMicros, TimeUnit.MICROSECONDS)
                }
            }

            override val cursorAttributes get() = inner.cursorAttributes
            override val explainAnalyze get() = this

            @Suppress("RedundantOverride")
            override fun forEachRemaining(action: Consumer<in RelationReader>?) = super.forEachRemaining(action)
            override fun getExactSizeIfKnown(): Long = inner.exactSizeIfKnown
            override fun hasCharacteristics(characteristics: Int): Boolean = inner.hasCharacteristics(characteristics)
            override fun getComparator(): Comparator<in RelationReader>? = inner.comparator
        }

        private class InterruptibleCursor(private val inner: ICursor) : ICursor by inner {
            override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
                if (Thread.interrupted()) throw InterruptedException()
                return inner.tryAdvance(c)
            }

            @Suppress("RedundantOverride")
            override fun forEachRemaining(action: Consumer<in RelationReader>?) = super.forEachRemaining(action)
            override fun getExactSizeIfKnown(): Long = inner.exactSizeIfKnown
            override fun hasCharacteristics(characteristics: Int): Boolean = inner.hasCharacteristics(characteristics)
            override fun getComparator(): Comparator<in RelationReader>? = inner.comparator
        }

        /**
         * Throws [InterruptedException] from [tryAdvance] if the calling thread has been interrupted, before pulling from this cursor.
         *
         * Wrap a child cursor with this where its parent pulls from it, so that a cancelled query stops at the next batch boundary.
         */
        @JvmStatic
        fun ICursor.wrapInterruptible(): ICursor = InterruptibleCursor(this)

        @JvmStatic
        fun ICursor.wrapTracing(tracer: Tracer?, span: Span?): ICursor = TracingCursor(this, tracer, span)

        @JvmStatic
        fun ICursor.wrapTracing(tracer: Tracer?, span: Span?, spanName: String?): ICursor =
            TracingCursor(this, tracer, span, spanName)

        @JvmStatic
        fun ICursor.wrapTracing(tracer: Tracer?, span: Span?, spanName: String?, pushdowns: Map<String, Any>?): ICursor =
            TracingCursor(this, tracer, span, spanName, pushdowns)
    }
}