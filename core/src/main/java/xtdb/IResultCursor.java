package xtdb;

import clojure.lang.Symbol;
import java.util.Map;

public interface IResultCursor<E> extends ICursor<E> {
    Map<Symbol, Object> columnTypes();
}
