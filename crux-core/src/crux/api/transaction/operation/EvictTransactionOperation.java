package crux.api.transaction.operation;

import clojure.lang.PersistentVector;
import crux.api.document.CruxId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EvictTransactionOperation extends TransactionOperation {
    private final Object id;

    private EvictTransactionOperation(Object id) {
        super(Type.EVICT);
        this.id = CruxId.validate(id);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvictTransactionOperation that = (EvictTransactionOperation) o;
        return type.equals(that.type)
                && CruxId.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id);
    }
}
