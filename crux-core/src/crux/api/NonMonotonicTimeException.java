package crux.api;

public class NonMonotonicTimeException extends Exception {
    private static final long serialVersionUID = 2631461243595643075L;

    public NonMonotonicTimeException(String message) {
        super(message);
    }
}
