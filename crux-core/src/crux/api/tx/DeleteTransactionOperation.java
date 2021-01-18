package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;

import java.util.Date;
import java.util.Objects;

public final class DeleteTransactionOperation extends TransactionOperation {
    public static DeleteTransactionOperation factory(Object id) {
        return new DeleteTransactionOperation(id, null, null);
    }

    public static DeleteTransactionOperation factory(Object id, Date validTime) {
        return new DeleteTransactionOperation(id, validTime, null);
    }

    public static DeleteTransactionOperation factory(Object id, Date validTime, Date endValidTime) {
        return new DeleteTransactionOperation(id, validTime, endValidTime);
    }

    private final Object id;
    private final Date validTime;
    private final Date endValidTime;

    private DeleteTransactionOperation(Object id, Date validTime, Date endValidTime) {
        this.id = id;
        this.validTime = validTime;
        this.endValidTime = endValidTime;
    }

    @Override
    public final IPersistentVector toVector() {
        IPersistentVector ret = PersistentVector.EMPTY
                .cons(Type.DELETE.getKeyword())
                .cons(id);
        if (validTime == null) return ret;
        ret = ret.cons(validTime);
        if (endValidTime == null) return ret;
        return ret.cons(endValidTime);
    }

    @Override
    public final Type getType() {
        return Type.DELETE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteTransactionOperation that = (DeleteTransactionOperation) o;
        return id.equals(that.id)
                && Objects.equals(validTime, that.validTime)
                && Objects.equals(endValidTime, that.endValidTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Type.DELETE, id, validTime, endValidTime);
    }
}
