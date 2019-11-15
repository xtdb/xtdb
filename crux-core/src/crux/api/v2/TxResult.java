package crux.api.v2;

import java.util.Date;

public class TxResult {
    public final Date txTime;
    public final Long txId;

    private TxResult(Date txTime, Long txId) {
        this.txTime = txTime;
        this.txId = txId;
    }

    public static TxResult txResult(Date txTime, Long txId) {
        return new TxResult(txTime, txId);
    }

}
