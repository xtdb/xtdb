package xtdb.api;

import java.time.Instant;
import java.util.Objects;

public class TransactionKey implements Comparable<TransactionKey> {
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
}