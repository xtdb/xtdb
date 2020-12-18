package crux.api.alpha;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;
import crux.api.alphav2.CruxId;

/*
@Deprecated
public class EvictOperation extends TransactionOperation {
    private static final Keyword TX_EVICT = crux.api.alpha.Util.keyword("crux.tx/evict");

    private final CruxId evictId;

    EvictOperation(CruxId evictId) {
        this.evictId = evictId;
    }

    @Override
    protected PersistentVector toEdn() {
        return PersistentVector.create(TX_EVICT, evictId.toEdn());
    }
}*/
