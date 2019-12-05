package crux.api.alpha;

import clojure.lang.IDeref;
import clojure.lang.Keyword;

import java.util.Map;

public class AsyncTxResult {
    private final IDeref result;

    private AsyncTxResult(IDeref result) {
        this.result = result;
    }

    static AsyncTxResult asyncTxResult(IDeref result) {
        return new AsyncTxResult(result);
    }

    @SuppressWarnings("unchecked")
    public TxResult deref() {
        return TxResult.txResult((Map<Keyword, ?>) result.deref());
    }
}
