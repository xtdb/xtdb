package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;

import java.util.Objects;

public final class EvictOperation extends TransactionOperation {
    public static EvictOperation create(Object id) {
        return new EvictOperation(id);
    }

    public Object getId() {
        return id;
    }

    private final Object id;

    private EvictOperation(Object id) {
        this.id = id;
    }

    @Override
    public final Type getType() {
        return Type.EVICT;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvictOperation that = (EvictOperation) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Type.EVICT, id);
    }
}
