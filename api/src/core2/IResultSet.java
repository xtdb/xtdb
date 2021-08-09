package core2;

import java.util.Iterator;

public interface IResultSet<E> extends Iterator<E>, AutoCloseable {
    @Override
    default void close() {
    }
}
