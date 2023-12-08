package xtdb;

import clojure.lang.IExceptionInfo;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;

@SuppressWarnings("unused")
public class IllegalArgumentException extends java.lang.IllegalArgumentException implements IExceptionInfo {
    private static final long serialVersionUID = 8569715935234823692L;
    private static final Keyword illegalArgument = Keyword.intern("illegal-argument");
    private static final Keyword errorType = Keyword.intern("xtdb.error", "error-type");
    private static final Keyword errorKey = Keyword.intern("xtdb.error","error-key");
    private static final Keyword messageKey = Keyword.intern("message");

    private final IPersistentMap data;

    public IllegalArgumentException(String message, IPersistentMap data, Throwable cause) {
        super(message, cause);
        this.data = data;
    }

    @Override
    public IPersistentMap getData() {
        return data;
    }
    public static IllegalArgumentException create(Keyword error, PersistentHashMap data) {
        return create(error, data,null);
    }
    public static IllegalArgumentException create(Keyword error, PersistentHashMap data, Throwable cause) {
        String message = String.format("Illegal argument: '%s'", error.toString());
        return new IllegalArgumentException(message, data.assoc(errorType, illegalArgument).assoc(errorKey, error).assoc(messageKey, message), cause);
    }
}
