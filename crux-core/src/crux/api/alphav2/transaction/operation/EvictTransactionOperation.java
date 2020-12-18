package crux.api.alphav2.transaction.operation;

import clojure.lang.PersistentVector;
import crux.api.alphav2.CruxId;

import java.util.List;

public class EvictTransactionOperation extends TransactionOperation {
    private final CruxId id;

    public EvictTransactionOperation(CruxId id) {
        super(Type.EVICT);
        this.id = id;
    }

    public static EvictTransactionOperation factory(PersistentVector vector) {
        Object rawId = vector.get(1);
        CruxId id = CruxId.cruxId(rawId);
        return new EvictTransactionOperation(id);
    }

    @Override
    List<Object> getArgs() {
        return List.of(id);
    }
}
