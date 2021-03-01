package core2;

import java.util.function.Consumer;

public interface ICursor<E> extends AutoCloseable {
    boolean tryAdvance(Consumer<? super E> action);

    @Override
    default void close() {
    }
}
