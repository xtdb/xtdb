package crux;

import clojure.lang.IExceptionInfo;
import clojure.lang.IPersistentMap;

@SuppressWarnings("unused")
public class IllegalArgumentException extends java.lang.IllegalArgumentException implements IExceptionInfo {
    private static final long serialVersionUID = 8569715935234823692L;

    private final IPersistentMap data;

    public IllegalArgumentException(String message, IPersistentMap data, Throwable cause) {
        super(message, cause);
        this.data = data;
    }

    @Override
    public IPersistentMap getData() {
        return data;
    }
}
