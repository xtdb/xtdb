package crux.api.transaction.operation;

import clojure.lang.IPersistentMap;
import clojure.lang.PersistentVector;
import crux.api.document.CruxDocument;
import crux.api.document.CruxId;
import crux.api.document.ICruxDocument;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class MatchTransactionOperation extends TransactionOperation {
    private final Object id;
    private final ICruxDocument document;
    private final Date validTime;

    private MatchTransactionOperation(Object id, ICruxDocument document, Date validTime) {
        super(Type.MATCH);
        this.id = CruxId.validate(id);
        this.document = document;
        this.validTime = validTime;
    }

    public static MatchTransactionOperation factory(PersistentVector vector) {
        Object id = vector.get(1);
        ICruxDocument document = null;
        Date validTime = null;

        Object o1 = vector.get(2);
        if (o1 instanceof IPersistentMap) {
            document = CruxDocument.factory((IPersistentMap) o1);
        }

        if (vector.size() > 3) {
            validTime = (Date) vector.get(3);
        }

        return new MatchTransactionOperation(id, document, validTime);
    }

    public static MatchTransactionOperation factoryNotExists(Object id) {
        return factoryNotExists(id, null);
    }

    public static MatchTransactionOperation factoryNotExists(Object id, Date validTime) {
        return new MatchTransactionOperation(id, null, validTime);
    }

    public static MatchTransactionOperation factory(Object id, ICruxDocument document) {
        return factory(id, document, null);
    }

    public static MatchTransactionOperation factory(Object id, ICruxDocument document, Date validTime) {
        return new MatchTransactionOperation(id, document, validTime);
    }

    @Override
    List<Object> getArgs() {
        ArrayList<Object> ret = new ArrayList<>();
        ret.add(id);

        if (document == null) {
            ret.add(null);
        }
        else {
            ret.add(document.toEdn());
        }

        if (validTime != null) {
            ret.add(validTime);
        }

        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchTransactionOperation that = (MatchTransactionOperation) o;
        return type.equals(that.type)
                && CruxId.equals(id, that.id)
                && ICruxDocument.equals(document, that.document)
                && Objects.equals(validTime, that.validTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id, document, validTime);
    }
}
