package xtdb.query

import io.micrometer.core.instrument.Timer
import xtdb.api.ICursor
import xtdb.api.ResultCursor
import xtdb.api.metrics.ConnectionMetrics
import xtdb.arrow.RelationReader
import java.util.Comparator
import java.util.Spliterator
import java.util.function.Consumer

/**
 * Records `query.timer` and `query.error` for a cursor opened through an [xtdb.api.Xtdb.Connection], so that every
 * frontend - pgwire, in-process ADBC, Flight SQL - is instrumented alike.
 *
 * The timer spans the cursor's lifetime, which is a query's duration as its client sees it: the query is planned as
 * the cursor opens, then rows stream out until it's closed.
 *
 * Deliberately not a `ResultCursor by inner`: that would route [ICursor.consume] and [Spliterator.forEachRemaining]
 * into the inner cursor's own advancing, past the [tryAdvance] below, and a consumer's errors would go uncounted.
 * Here they're inherited so they drive this cursor instead.
 */
class MeteredCursor(
    private val inner: ResultCursor,
    private val metrics: ConnectionMetrics,
) : ResultCursor {

    private val sample = Timer.start()
    private var recorded = false

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
        try {
            inner.tryAdvance(c)
        } catch (e: Throwable) {
            metrics.queryErrorCounter.increment()
            throw e
        }

    override val resultTypes get() = inner.resultTypes
    override val cursorType get() = inner.cursorType
    override val childCursors get() = inner.childCursors
    override val explainAnalyze get() = inner.explainAnalyze
    override val cursorAttributes get() = inner.cursorAttributes

    override fun characteristics() = inner.characteristics()
    override fun estimateSize() = inner.estimateSize()
    override fun getExactSizeIfKnown() = inner.exactSizeIfKnown
    override fun hasCharacteristics(characteristics: Int) = inner.hasCharacteristics(characteristics)
    override fun getComparator(): Comparator<in RelationReader>? = inner.comparator

    override fun close() {
        try {
            inner.close()
        } finally {
            if (!recorded) {
                recorded = true
                sample.stop(metrics.queryTimer)
            }
        }
    }
}
