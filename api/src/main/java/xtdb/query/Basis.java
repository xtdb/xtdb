package xtdb.query;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import clojure.lang.*;
import xtdb.api.TransactionKey;

public class Basis implements ILookup, Seqable {
    private static final Keyword AT_TX_KEY = Keyword.intern("at-tx");
    private static final Keyword CURRENT_TIME_KEY = Keyword.intern("current-time");

    private final TransactionKey atTx;
    private final Instant currentTime;

    public Basis(TransactionKey atTx, Instant currentTime) {
        this.atTx = atTx;
        this.currentTime = currentTime;
    }

    public TransactionKey atTx() {
        return atTx;
    }

    public Instant currentTime() {
        return currentTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Basis basis = (Basis) o;
        return atTx.equals(basis.atTx) && currentTime.equals(basis.currentTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(atTx, currentTime);
    }

    @Override
    public String toString() {
        return "Basis{" +
                "atTx=" + atTx +
                ", currentTime=" + currentTime +
                '}';
    }

    @Override
    public Object valAt(Object key) {
        return valAt(key, null);
    }

    @Override
    public Object valAt(Object key, Object notFound) {
        if (key == AT_TX_KEY) {
            return atTx;
        } else if (key == CURRENT_TIME_KEY) {
            return currentTime;
        } else {
            return notFound;
        }
    }

    @Override
    public ISeq seq() {
        List<Object> seqList = new ArrayList<>();
        if (atTx != null) {
            seqList.add(MapEntry.create(AT_TX_KEY, atTx));
        }
        if (currentTime != null) {
            seqList.add(MapEntry.create(CURRENT_TIME_KEY, currentTime));
        }
        return PersistentList.create(seqList).seq();
    }
}