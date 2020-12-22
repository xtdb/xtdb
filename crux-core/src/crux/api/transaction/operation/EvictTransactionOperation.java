package crux.api.transaction.operation;

import clojure.lang.PersistentVector;
import crux.api.exception.CruxIdException;

import java.util.ArrayList;
import java.util.List;

public class EvictTransactionOperation extends TransactionOperation {
    private final Object id;

    private EvictTransactionOperation(Object id) {
        super(Type.EVICT);
        CruxIdException.assertValidType(id);
        this.id = id;
    }

    public static EvictTransactionOperation factory(PersistentVector vector) {
        Object id = vector.get(1);
        return new EvictTransactionOperation(id);
    }

    public static EvictTransactionOperation factory(Object id) {
        return new EvictTransactionOperation(id);
    }

    @Override
    List<Object> getArgs() {
        ArrayList<Object> ret = new ArrayList<>();
        ret.add(id);
        return ret;
    }
}
