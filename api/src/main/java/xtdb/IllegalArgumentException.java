package xtdb;

import clojure.lang.IExceptionInfo;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;

import java.util.Objects;

@SuppressWarnings("unused")
public class IllegalArgumentException extends java.lang.IllegalArgumentException implements IExceptionInfo {
    private static final long serialVersionUID = 8569715935234823692L;
    private static final Keyword ILLEGAL_ARGUMENT = Keyword.intern("illegal-argument");
    private static final Keyword ERROR_TYPE = Keyword.intern("xtdb.error", "error-type");
    private static final Keyword ERROR_KEY = Keyword.intern("xtdb.error","error-key");
    private static final Keyword MESSAGE_KEY = Keyword.intern("xtdb.error", "message");

    private final IPersistentMap data;

    public IllegalArgumentException(String message, IPersistentMap data, Throwable cause) {
        super(message, cause);
        this.data = data;
    }

    @Override
    public IPersistentMap getData() {
        return data;
    }

    public static IllegalArgumentException create(Keyword error, String message, IPersistentMap data) {
        return create(error, message, data,null);
    }
    public static IllegalArgumentException create(Keyword error, String message, IPersistentMap data, Throwable cause) {
        message = message != null ? message : (String) data.valAt(MESSAGE_KEY, String.format("Illegal argument: '%s'", error.toString()));
        return new IllegalArgumentException(message, data.assoc(ERROR_TYPE, ILLEGAL_ARGUMENT).assoc(ERROR_KEY, error).assoc(MESSAGE_KEY, message), cause);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (IllegalArgumentException) o;
        return Objects.equals(getMessage(), that.getMessage()) && Objects.equals(getCause(), that.getCause()) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMessage(), getCause(), data);
    }

    @Override
    public String toString() {
        return String.format("IllegalArgumentException{message=%s, data=%s}", getMessage(), data);
    }
}
