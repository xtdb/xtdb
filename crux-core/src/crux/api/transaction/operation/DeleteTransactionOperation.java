package crux.api.transaction.operation;

import clojure.lang.PersistentVector;
import crux.api.document.CruxId;
import crux.api.exception.CruxIdException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class DeleteTransactionOperation extends TransactionOperation {
    private final Object id;
    private final Date validTime;
    private final Date endValidTime;

    private DeleteTransactionOperation(Object id, Date validTime, Date endValidTime) {
        super(Type.DELETE);
        this.id = CruxId.validate(id);
        this.validTime = validTime;
        this.endValidTime = endValidTime;
    }

    public static DeleteTransactionOperation factory(PersistentVector vector) {
        Object id = vector.get(1);
        Date validTime = null;
        Date endValidTime = null;
        if (vector.size() > 2) {
            validTime = (Date) vector.get(2);
        }
        if (vector.size() > 3) {
            endValidTime = (Date) vector.get(3);
        }

        return new DeleteTransactionOperation(id, validTime, endValidTime);
    }

    public static DeleteTransactionOperation factory(Object id) {
        return factory(id, null);
    }

    public static DeleteTransactionOperation factory(Object id, Date validTime) {
        return factory(id, validTime, null);
    }

    public static DeleteTransactionOperation factory(Object id, Date validTime, Date endValidTime) {
        return new DeleteTransactionOperation(id, validTime, endValidTime);
    }

    @Override
    List<Object> getArgs() {
        ArrayList<Object> ret = new ArrayList<>();
        ret.add(id);
        if (endValidTime != null) {
            ret.add(validTime);
            ret.add(endValidTime);
        }
        else if (validTime != null) {
            ret.add(validTime);
        }
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteTransactionOperation that = (DeleteTransactionOperation) o;
        return type.equals(that.type)
                && CruxId.equals(id, that.id)
                && Objects.equals(validTime, that.validTime)
                && Objects.equals(endValidTime, that.endValidTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id, validTime, endValidTime);
    }
}
