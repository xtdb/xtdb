package crux.api.tx;

import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import crux.api.AbstractCruxDocument;

import java.util.Date;
import java.util.Objects;

public final class MatchOperation extends TransactionOperation {
    public static MatchOperation create(Object id) {
        return new MatchOperation(id, null, null);
    }

    public static MatchOperation create(Object id, Date validTime) {
        return new MatchOperation(id, null, validTime);
    }

    public static MatchOperation create(AbstractCruxDocument document) {
        return new MatchOperation(document.getId(), document, null);
    }

    public static MatchOperation create(AbstractCruxDocument document, Date validTime) {
        return new MatchOperation(document.getId(), document, validTime);
    }

    private final Object id;
    private final AbstractCruxDocument compare;
    private final Date validTime;

    private MatchOperation(Object id, AbstractCruxDocument compare, Date validTime) {
        this.id = id;
        this.compare = compare;
        this.validTime = validTime;
    }

    @Override
    public final IPersistentVector toVector() {
        IPersistentVector ret = PersistentVector.EMPTY
                .cons(Type.MATCH.getKeyword())
                .cons(id);

        if (compare == null) {
            ret = ret.cons(null);
        }
        else {
            ret = ret.cons(compare.toMap());
        }

        if (validTime == null) return ret;
        return ret.cons(validTime);
    }

    @Override
    public final Type getType() {
        return Type.MATCH;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchOperation that = (MatchOperation) o;
        return id.equals(that.id)
                && Objects.equals(compare, that.compare)
                && Objects.equals(validTime, that.validTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Type.MATCH, id, compare, validTime);
    }
}
