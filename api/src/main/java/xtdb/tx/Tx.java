package xtdb.tx;

import java.util.List;
import java.util.Objects;

public class Tx {
    private List<Ops> txOps;
    private TxOptions txOptions;

    public Tx(List<Ops> txOps, TxOptions txOptions) {
        this.txOps = txOps;
        this.txOptions = txOptions;
    }

    public List<Ops> txOps() {
        return txOps;
    }

    public TxOptions txOptions() {
        return txOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tx tx = (Tx) o;
        return Objects.equals(txOps, tx.txOps) && Objects.equals(txOptions, tx.txOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txOps, txOptions);
    }

    @Override
    public String toString() {
        return "Tx{" +
                "txOps=" + txOps +
                ", txOptions=" + txOptions +
                '}';
    }
}
