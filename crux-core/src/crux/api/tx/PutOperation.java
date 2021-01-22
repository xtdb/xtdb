package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import crux.api.AbstractCruxDocument;

import java.util.Date;
import java.util.Objects;

public final class PutOperation extends TransactionOperation {
    public static PutOperation create(AbstractCruxDocument document) {
        return new PutOperation(document, null, null);
    }

    public static PutOperation create(AbstractCruxDocument document, Date startValidTime) {
        return new PutOperation(document, startValidTime, null);
    }

    public static PutOperation create(AbstractCruxDocument document, Date startValidTime, Date endValidTime) {
        return new PutOperation(document, startValidTime, endValidTime);
    }

    private final AbstractCruxDocument document;
    private final Date startValidTime;
    private final Date endValidTime;

    private PutOperation(AbstractCruxDocument document, Date startValidTime, Date endValidTime) {
        this.document = document;
        this.startValidTime = startValidTime;
        this.endValidTime = endValidTime;
    }

    @Override
    public final IPersistentVector toVector() {
        IPersistentVector ret = PersistentVector.EMPTY
                .cons(Type.PUT.getKeyword())
                .cons(document.toMap());
        if (startValidTime == null) return ret;
        ret = ret.cons(startValidTime);
        if (endValidTime == null) return ret;
        return ret.cons(endValidTime);
    }

    @Override
    public final Type getType() {
        return Type.PUT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutOperation that = (PutOperation) o;
        return document.equals(that.document)
                && Objects.equals(startValidTime, that.startValidTime)
                && Objects.equals(endValidTime, that.endValidTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Type.PUT, document, startValidTime, endValidTime);
    }
}
