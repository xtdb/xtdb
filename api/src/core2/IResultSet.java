package core2;

import clojure.lang.Symbol;

import java.util.Iterator;
import java.util.Map;

public interface IResultSet<E> extends Iterator<E>, AutoCloseable {

    Map<Symbol, Object> columnTypes();

    @Override
    default void close() {
    }
}
