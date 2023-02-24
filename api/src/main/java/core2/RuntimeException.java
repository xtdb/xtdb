package core2;

import clojure.lang.IExceptionInfo;
import clojure.lang.IPersistentMap;

@SuppressWarnings("unused")
public class RuntimeException extends java.lang.RuntimeException implements IExceptionInfo {
    private static final long serialVersionUID = 7375922113394623815L;

    private final IPersistentMap data;

    public RuntimeException(String message, IPersistentMap data, Throwable cause) {
        super(message, cause);
        this.data = data;
    }

    @Override
    public IPersistentMap getData() {
        return data;
    }
}
