package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import crux.api.AbstractCruxDocument;

import java.util.Date;
import java.util.Objects;

public final class PutTransactionOperation extends TransactionOperation {
    public static PutTransactionOperation factory(AbstractCruxDocument document) {
        return new PutTransactionOperation(document, null, null);
    }

    public static PutTransactionOperation factory(AbstractCruxDocument document, Date validTime) {
        return new PutTransactionOperation(document, validTime, null);
    }

    public static PutTransactionOperation factory(AbstractCruxDocument document, Date validTime, Date endValidTime) {
        return new PutTransactionOperation(document, validTime, endValidTime);
    }

    private final AbstractCruxDocument document;
    private final Date validTime;
    private final Date endValidTime;

    private PutTransactionOperation(AbstractCruxDocument document, Date validTime, Date endValidTime) {
        this.document = document;
        this.validTime = validTime;
        this.endValidTime = endValidTime;
    }

    @Override
    public final IPersistentVector toVector() {
        IPersistentVector ret = PersistentVector.EMPTY
                .cons(Type.PUT.getKeyword())
                .cons(document.toMap());
        if (validTime == null) return ret;
        ret = ret.cons(validTime);
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
        PutTransactionOperation that = (PutTransactionOperation) o;
        return document.equals(that.document)
                && Objects.equals(validTime, that.validTime)
                && Objects.equals(endValidTime, that.endValidTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Type.PUT, document, validTime, endValidTime);
    }
}
