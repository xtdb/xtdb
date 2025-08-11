package xtdb

import io.micrometer.core.instrument.Counter
import org.apache.arrow.vector.types.pojo.Field
import java.util.function.Consumer

interface IResultCursor<E> : ICursor<E> {
    val resultFields: List<Field>

    class ErrorTrackingCursor<E>(
        private val inner: IResultCursor<E>,
        private val counter: Counter
    ) : IResultCursor<E> by inner {
        override fun tryAdvance(c: Consumer<in E>): Boolean =
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
