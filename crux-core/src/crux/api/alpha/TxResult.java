package crux.api.alpha;

import java.util.Date;

public class TxResult {
    public final Date txTime;
    public final long txId;

    private TxResult(Date txTime, long txId) {
        this.txTime = txTime;
        this.txId = txId;
    }

    public static TxResult txResult(Date txTime, long txId) {
        return new TxResult(txTime, txId);
    }

}
