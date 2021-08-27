package xtdb.api.tx;

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

    public Object getId() {
        return id;
    }

    public Date getStartValidTime() {
        return startValidTime;
    }

    public Date getEndValidTime() {
        return endValidTime;
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
    public <E> E accept(Visitor<E> visitor) {
        return visitor.visit(this);
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
        return Objects.hash("delete", id, startValidTime, endValidTime);
    }
}
