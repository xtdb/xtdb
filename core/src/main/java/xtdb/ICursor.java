package xtdb;

import java.util.Spliterator;

public interface ICursor<E> extends Spliterator<E>, AutoCloseable {
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
