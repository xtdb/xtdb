package xtdb.query;

import xtdb.api.TransactionKey;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

public class QueryOpts {
    private Map<String, Object> args;
    private Basis basis;
    private TransactionKey afterTx;
    private Duration txTimeout;
    private ZoneId defaultTz;
    private boolean explain;
    private String keyFn;

    public QueryOpts(Map<String, Object> args, Basis basis, TransactionKey afterTx, Duration txTimeout,
                        ZoneId defaultTz, boolean explain, String keyFn) {
        this.args = args;
        this.basis = basis;
        this.afterTx = afterTx;
        this.txTimeout = txTimeout;
        this.defaultTz = defaultTz;
        this.explain = explain;
        this.keyFn = keyFn;
    }

    public Map<String, Object> args() {
        return args;
    }

    public Basis basis() {
        return basis;
    }

    public TransactionKey afterTx() {
        return afterTx;
    }

    public Duration txTimeout() {
        return txTimeout;
    }

    public ZoneId defaultTz() {
        return defaultTz;
    }

    public boolean explain() {
        return explain;
    }

    public String keyFn() {
        return keyFn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryOpts queryOpts = (QueryOpts) o;
        return Objects.equals(args, queryOpts.args) &&
                Objects.equals(basis, queryOpts.basis) &&
                Objects.equals(afterTx, queryOpts.afterTx) &&
                Objects.equals(txTimeout, queryOpts.txTimeout) &&
                Objects.equals(defaultTz, queryOpts.defaultTz) &&
                Objects.equals(explain, queryOpts.explain) &&
                Objects.equals(keyFn, queryOpts.keyFn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(args, basis, afterTx, txTimeout, defaultTz, explain, keyFn);
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
                "args=" + args +
                ", basis=" + basis +
                ", afterTx=" + afterTx +
                ", txTimeout=" + txTimeout +
                ", defaultTz=" + defaultTz +
                ", explain=" + explain +
                ", keyFn=" + keyFn +
                '}';
    }
}
