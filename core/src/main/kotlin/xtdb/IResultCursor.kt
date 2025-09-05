package xtdb

import io.micrometer.core.instrument.Counter
import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.RelationReader
import java.util.function.Consumer

interface IResultCursor : ICursor {
    val resultFields: List<Field>

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
