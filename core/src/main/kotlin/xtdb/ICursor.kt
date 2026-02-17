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
        val pushdowns: Map<String, Any>?
        val cursorAttributes: Map<String, String>?
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

        private class TracingCursor(
            private val inner: ICursor,
            private val tracer: Tracer?,
            private val parentSpan: Span?,
            private val attributes: Map<String, String>? = null,
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
                        span = tracer.nextSpan(parentSpan)!!
                            .name(spanName ?: "query.cursor.${inner.cursorType}")
                            .tag("cursor.type", inner.cursorType)
                            .apply { attributes?.forEach { (k, v) -> tag(k, v) } }
                            .start()
                    }
                }
                val pageStart = clock.instant()

                return inner.tryAdvance { rel ->
                    val pageTime = Duration.between(pageStart, clock.instant())
                    timeToFirstPage = timeToFirstPage ?: pageTime
                    rowCount += rel.rowCount
                    pageCount++
                    c.accept(rel)
                }.also {
                    val pageEnd = clock.instant()
                    totalTime += Duration.between(pageStart, pageEnd)
                    endTime = pageEnd
                }
            }

            override fun close() {
                inner.close()
                span?.let { s ->
                    s.tag("cursor.total_time_ms", totalTime.toMillis().toString())
                    timeToFirstPage?.let { ttfp ->
                        s.tag("cursor.time_to_first_page_ms", ttfp.toMillis().toString())
                    }
                    s.tag("cursor.page_count", pageCount.toString())
                    s.tag("cursor.row_count", rowCount.toString())
                    s.end(endTime.asMicros, java.util.concurrent.TimeUnit.MICROSECONDS)
                }
            }

            override val cursorAttributes get() = attributes
            override val explainAnalyze get() = this

            @Suppress("RedundantOverride")
            override fun forEachRemaining(action: Consumer<in RelationReader>?) = super.forEachRemaining(action)
            override fun getExactSizeIfKnown(): Long = inner.exactSizeIfKnown
            override fun hasCharacteristics(characteristics: Int): Boolean = inner.hasCharacteristics(characteristics)
            override fun getComparator(): Comparator<in RelationReader>? = inner.comparator
        }

        @JvmStatic
        fun ICursor.wrapTracing(tracer: Tracer?, span: Span?): ICursor = TracingCursor(this, tracer, span)

        @JvmStatic
        fun ICursor.wrapTracing(tracer: Tracer?, span: Span?, attributes: Map<String, String>?): ICursor =
            TracingCursor(this, tracer, span, attributes)

        @JvmStatic
        fun ICursor.wrapTracing(tracer: Tracer?, span: Span?, attributes: Map<String, String>?, spanName: String?): ICursor =
            TracingCursor(this, tracer, span, attributes, spanName)

        @JvmStatic
        fun ICursor.wrapTracing(tracer: Tracer?, span: Span?, attributes: Map<String, String>?, spanName: String?, pushdowns: Map<String, Any>?): ICursor =
            TracingCursor(this, tracer, span, attributes, spanName, pushdowns)
    }
}
