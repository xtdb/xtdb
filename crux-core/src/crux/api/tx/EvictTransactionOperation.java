package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;

import java.util.Objects;

public final class EvictTransactionOperation extends TransactionOperation {
    public static EvictTransactionOperation factory(Object id) {
        return new EvictTransactionOperation(id);
    }

    private final Object id;

    private EvictTransactionOperation(Object id) {
        this.id = id;
    }

    @Override
    public final IPersistentVector toVector() {
        return PersistentVector.EMPTY
                .cons(Type.EVICT.getKeyword())
                .cons(id);
    }

    @Override
    public final Type getType() {
        return Type.EVICT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvictTransactionOperation that = (EvictTransactionOperation) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Type.EVICT, id);
    }
}
