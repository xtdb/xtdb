package xtdb.query;

import java.time.Instant;
import java.util.Objects;

import xtdb.api.TransactionKey;

public class Basis {
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
}