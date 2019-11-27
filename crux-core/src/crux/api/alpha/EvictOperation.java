package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import static crux.api.alpha.Util.keyword;

public class EvictOperation extends TransactionOperation {
    private final CruxId evictId;

    EvictOperation(CruxId evictId) {
        this.evictId = evictId;
    }

    @Override
    protected PersistentVector toEdn() {
        return PersistentVector.create(TX_EVICT, evictId.toEdn());
    }
}
