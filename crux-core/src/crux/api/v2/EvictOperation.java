package crux.api.v2;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import static crux.api.v2.Util.keyword;

public class EvictOperation extends Operation {
    private static final Keyword TX_EVICT = keyword("crux.tx/evict");

    private final CruxId evictId;

    private EvictOperation(CruxId evictId) {
        this.evictId = evictId;
    }

    public static EvictOperation evictOp(CruxId evictId) {
        return new EvictOperation(evictId);
    }

    @Override
    protected PersistentVector toEdn() {
        return PersistentVector.create(TX_EVICT, evictId.toEdn());
    }
}
