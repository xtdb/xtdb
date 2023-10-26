package xtdb;

import clojure.lang.Symbol;

import java.util.Map;

public interface IResultCursor<E> extends ICursor<E> {
    @Deprecated
    Map<Symbol, Object> columnTypes();
    Map<Symbol, Object> columnFields();
}
