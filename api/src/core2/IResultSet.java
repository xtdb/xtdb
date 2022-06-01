package core2;

import java.util.Iterator;
import java.util.Map;

public interface IResultSet<E> extends Iterator<E>, AutoCloseable {

    Map<String, Object> columnTypes();

    @Override
    default void close() {
    }
}
