package xtdb.query

import io.micrometer.core.instrument.Counter
import xtdb.api.ResultCursor
import xtdb.arrow.RelationReader
import java.util.Comparator
import java.util.function.Consumer

class ErrorTrackingCursor(
    private val inner: ResultCursor,
    private val counter: Counter
) : ResultCursor by inner {
    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
        try {
            inner.tryAdvance(c)
        } catch (e: Throwable) {
            counter.increment()
            throw e
        }

    override fun forEachRemaining(action: Consumer<in RelationReader>?) = inner.forEachRemaining(action)
    override fun getExactSizeIfKnown() = inner.exactSizeIfKnown
    override fun hasCharacteristics(characteristics: Int) = inner.hasCharacteristics(characteristics)
    override fun getComparator(): Comparator<in RelationReader>? = inner.comparator
}