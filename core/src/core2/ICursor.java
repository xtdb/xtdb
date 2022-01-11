package core2;

import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

public interface ICursor<E> extends Spliterator<E>, AutoCloseable {
    Set<String> getColumnNames();

    @Override
    default Spliterator<E> trySplit() {
        return null;
    }

    @Override
    default int characteristics() {
        return IMMUTABLE;
    }

    @Override
    default long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    default void close() {
    }
}
