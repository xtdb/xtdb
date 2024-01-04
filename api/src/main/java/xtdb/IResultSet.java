package xtdb;

import java.util.Iterator;

public interface IResultSet<E> extends Iterator<E>, AutoCloseable {
    @Override
    void close();
}
