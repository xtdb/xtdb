package xtdb;

import clojure.lang.IExceptionInfo;
import clojure.lang.IPersistentMap;

@SuppressWarnings("unused")
public class IllegalStateException extends java.lang.IllegalStateException implements IExceptionInfo {
    private static final long serialVersionUID = -4075310562924928483L;

    private final IPersistentMap data;

    public IllegalStateException(String message, IPersistentMap data, Throwable cause) {
        super(message, cause);
        this.data = data;
    }

    @Override
    public IPersistentMap getData() {
        return data;
    }
}
