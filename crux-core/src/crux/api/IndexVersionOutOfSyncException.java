package crux.api;

@SuppressWarnings("unused")
public final class IndexVersionOutOfSyncException extends RuntimeException {
    private static final long serialVersionUID = 6124848552293819498L;

    public IndexVersionOutOfSyncException(String message) {
        super(message);
    }
}
