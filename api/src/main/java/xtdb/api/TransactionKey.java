package xtdb.api;

import clojure.lang.*;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class TransactionKey implements Comparable<TransactionKey>, ILookup, Seqable {
    private static final Keyword TX_ID_KEY = Keyword.intern("tx-id");
    private static final Keyword SYSTEM_TIME_KEY = Keyword.intern("system-time");

    public long txId;
    public Instant systemTime;

    public TransactionKey(long txId, Instant systemTime) {
        this.txId = txId;
        this.systemTime = systemTime;
    }

    @Override
    public int compareTo(TransactionKey otherKey) {
        return Long.compare(this.txId, otherKey.txId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionKey that = (TransactionKey) o;
        return txId == that.txId && Objects.equals(systemTime, that.systemTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txId, systemTime);
    }

    public TransactionKey withSystemTime(Instant systemTime) {
        return new TransactionKey(this.txId, systemTime);
    }

    @Override
    public Object valAt(Object key){
        return valAt(key, null);
    }
    @Override
    public Object valAt(Object key, Object notFound) {
        if (key == TX_ID_KEY) {
            return txId;
        } else if (key == SYSTEM_TIME_KEY) {
           return systemTime;
        } else {
            return notFound;
        }
    }

    @Override
    public ISeq seq() {
        return PersistentList.create(List.of(MapEntry.create(TX_ID_KEY, txId), MapEntry.create(SYSTEM_TIME_KEY, systemTime))).seq();
    }
}