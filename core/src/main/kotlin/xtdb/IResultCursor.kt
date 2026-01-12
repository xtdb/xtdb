package xtdb

import io.micrometer.core.instrument.Counter
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import java.util.function.Consumer
import java.util.SequencedMap

interface IResultCursor : ICursor {
    val resultTypes: SequencedMap<String, VectorType>

    class ErrorTrackingCursor(
        private val inner: IResultCursor,
        private val counter: Counter
    ) : IResultCursor by inner {
        override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
            try {
                inner.tryAdvance(c)
            } catch (e: Throwable) {
                counter.increment()
                throw e
            }

        override fun getExactSizeIfKnown(): Long = inner.exactSizeIfKnown
        override fun hasCharacteristics(characteristics: Int) = inner.hasCharacteristics(characteristics)
    }
}
