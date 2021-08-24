package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * A class representing the basis of a DB instance - a valid time and a transaction instant.
 */
public final class DBBasis {
    private static final Keyword VALID_TIME = Keyword.intern("xt/valid-time");
    private static final Keyword TX = Keyword.intern("xt/tx");

    private final Date validTime;
    private final TransactionInstant transactionInstant;

    public Date getValidTime() {
        return validTime;
    }

    public TransactionInstant getTransactionInstant() {
        return transactionInstant;
    }

    public DBBasis(Date validTime, TransactionInstant transactionInstant) {
        this.validTime = validTime;
        this.transactionInstant = transactionInstant;
    }

    /**
     * Not intended for public use, may be removed.
     * @param map
     */
    @SuppressWarnings("unchecked")
    public static DBBasis factory(IPersistentMap map) {
        Date validTime = (Date) map.valAt(VALID_TIME);
        TransactionInstant transactionInstant = TransactionInstant.factory((Map<Keyword, ?>) map.valAt(TX));
        return new DBBasis(validTime, transactionInstant);
    }

    /**
     * Not intended for public use, may be removed.
     */
    public IPersistentMap toMap() {
        IPersistentMap ret = PersistentArrayMap.EMPTY;
        if (transactionInstant != null) {
            ret = ret.assoc(TX, transactionInstant.toMap());
        }
        if (validTime != null) {
            ret = ret.assoc(VALID_TIME, validTime);
        }
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBBasis dbBasis = (DBBasis) o;
        return Objects.equals(validTime, dbBasis.validTime)
                && Objects.equals(transactionInstant, dbBasis.transactionInstant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(validTime, transactionInstant);
    }
}
