package crux.api;

public class IndexVersionOutOfSyncException extends Exception {
    public IndexVersionOutOfSyncException(String message) {
        super(message);
    }
}
