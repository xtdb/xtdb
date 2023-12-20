package xtdb;

import clojure.lang.IExceptionInfo;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;

import java.util.Objects;

@SuppressWarnings("unused")
public class RuntimeException extends java.lang.RuntimeException implements IExceptionInfo {
    private static final long serialVersionUID = 7375922113394623815L;
    private static final Keyword RUNTIME_EXCEPTION = Keyword.intern("runtime-error");
    private static final Keyword ERROR_TYPE = Keyword.intern("xtdb.error", "error-type");
    private static final Keyword ERROR_KEY = Keyword.intern("xtdb.error","error-key");
    private static final Keyword MESSAGE_KEY = Keyword.intern("xtdb.error", "message");

    private final IPersistentMap data;

    public RuntimeException(String message, IPersistentMap data, Throwable cause) {
        super(message, cause);
        this.data = data;
    }

    @Override
    public IPersistentMap getData() {
        return data;
    }

    public static RuntimeException create(Keyword error, String message, IPersistentMap data) {
        return create(error, message, data,null);
    }
    public static RuntimeException create(Keyword error, String message, IPersistentMap data, Throwable cause) {
        message = message != null ? message : (String) data.valAt(MESSAGE_KEY, String.format("Runtime error: '%s'", error.toString()));
        return new RuntimeException(message, data.assoc(ERROR_TYPE, RUNTIME_EXCEPTION).assoc(ERROR_KEY, error).assoc(MESSAGE_KEY, message), cause);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (RuntimeException) o;
        return Objects.equals(getMessage(), that.getMessage()) && Objects.equals(getCause(), that.getCause()) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMessage(), getCause(), data);
    }

    @Override
    public String toString() {
        return String.format("RuntimeException{message=%s, data=%s}", getMessage(), data);
    }
}
