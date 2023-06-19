package xtdb;

import clojure.lang.Symbol;

import java.util.Iterator;
import java.util.Map;

public interface IResultSet<E> extends Iterator<E>, AutoCloseable {

    Map<Symbol, Object> columnTypes();

    Map<Symbol, Object> outerProjection();

    @Override
    default void close() {
    }
}
