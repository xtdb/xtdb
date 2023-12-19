package xtdb.api;

import clojure.lang.*;

import java.time.Instant;
import java.util.Objects;

public class TransactionKey implements Comparable<TransactionKey>, ILookup, Seqable {
    private static Keyword txIdKey = Keyword.intern("tx-id");
    private static Keyword systemTimeKey = Keyword.intern("system-time");

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
        if (key == txIdKey) {
            return txId;
        } else if (key == systemTimeKey) {
           return systemTime;
        } else {
            return notFound;
        }
    }

    @Override
    public ISeq seq() {
        IPersistentList persistentList = PersistentList.EMPTY;
        return persistentList.cons(MapEntry.create(txIdKey, txId)).cons(MapEntry.create(systemTimeKey, systemTime)).seq();
    }
}