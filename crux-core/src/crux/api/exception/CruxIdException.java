package crux.api.exception;

public class CruxIdException extends RuntimeException {
    private static final long serialVersionUID = 908571272;

    public CruxIdException(String className) {
        super(className + " is not a valid CruxId type");
    }
}
