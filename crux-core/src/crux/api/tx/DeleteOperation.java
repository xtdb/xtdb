package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;

import java.util.Date;
import java.util.Objects;

public final class DeleteOperation extends TransactionOperation {
    public static DeleteOperation create(Object id) {
        return new DeleteOperation(id, null, null);
    }

    public static DeleteOperation create(Object id, Date startValidTime) {
        return new DeleteOperation(id, startValidTime, null);
    }

    public static DeleteOperation create(Object id, Date startValidTime, Date endValidTime) {
        return new DeleteOperation(id, startValidTime, endValidTime);
    }

    private final Object id;
    private final Date startValidTime;
    private final Date endValidTime;

    private DeleteOperation(Object id, Date startValidTime, Date endValidTime) {
        this.id = id;
        this.startValidTime = startValidTime;
        this.endValidTime = endValidTime;
    }

    @Override
    public final IPersistentVector toVector() {
        IPersistentVector ret = PersistentVector.EMPTY
                .cons(Type.DELETE.getKeyword())
                .cons(id);
        if (startValidTime == null) return ret;
        ret = ret.cons(startValidTime);
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
        DeleteOperation that = (DeleteOperation) o;
        return id.equals(that.id)
                && Objects.equals(startValidTime, that.startValidTime)
                && Objects.equals(endValidTime, that.endValidTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Type.DELETE, id, startValidTime, endValidTime);
    }
}
