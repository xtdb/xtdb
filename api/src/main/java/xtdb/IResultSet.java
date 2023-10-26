package xtdb;

import clojure.lang.Symbol;

import java.util.Iterator;
import java.util.Map;

public interface IResultSet<E> extends Iterator<E>, AutoCloseable {
    @Deprecated
    Map<Symbol, Object> columnTypes();
    Map<Symbol, Object> columnFields();

    @Override
    default void close() {
    }
}
