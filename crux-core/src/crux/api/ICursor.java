package crux.api;

import java.util.Iterator;
import java.io.Closeable;
import java.util.function.Function;

public interface ICursor<E> extends Iterator<E>, Closeable {
    default <R> ICursor<R> map(Function<E, R> map) {
        return new MappedCursor<>(this, map);
    }
}
