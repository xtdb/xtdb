package crux.api.exception;

public class CruxDocumentException extends RuntimeException {
    private static final long serialVersionUID = 731928179;
    public CruxDocumentException(String message) {
        super(message);
    }
}
