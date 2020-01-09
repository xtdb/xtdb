package crux.api;

import java.util.Date;

public class NodeOutOfSyncException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public final Date requestedTxTime;
    public final Date availableTxTime;

    public NodeOutOfSyncException(String message, Date requestedTxTime, Date availableTxTime) {
        super(message);
        this.requestedTxTime = requestedTxTime;
        this.availableTxTime = availableTxTime;
    }
}
