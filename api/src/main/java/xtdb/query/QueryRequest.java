package xtdb.query;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import xtdb.api.TransactionKey;

public class QueryRequest {
    private Query query;
    private Map<String, Object> args;
    private Basis basis;
    private TransactionKey afterTx;
    private Duration txTimeout;
    private ZoneId defaultTz;
    private boolean explain;
    private String keyFn;

    public QueryRequest(Query query, Map<String, Object> args, Basis basis, TransactionKey afterTx, Duration txTimeout,
                    ZoneId defaultTz, boolean explain, String keyFn) {
        this.query = query;
        this.args = args;
        this.basis = basis;
        this.afterTx = afterTx;
        this.txTimeout = txTimeout;
        this.defaultTz = defaultTz;
        this.explain = explain;
        this.keyFn = keyFn;
    }

    public Query query() {
        return query;
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
        QueryRequest queryMap = (QueryRequest) o;
        return Objects.equals(query, queryMap.query) &&
               Objects.equals(args, queryMap.args) &&
               Objects.equals(basis, queryMap.basis) &&
               Objects.equals(afterTx, queryMap.afterTx) &&
               Objects.equals(txTimeout, queryMap.txTimeout) &&
               Objects.equals(defaultTz, queryMap.defaultTz) &&
               Objects.equals(explain, queryMap.explain) &&
               Objects.equals(keyFn, queryMap.keyFn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, args, basis, afterTx, txTimeout, defaultTz, explain, keyFn);
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
                "query=" + query +
                ", args=" + args +
                ", basis=" + basis +
                ", afterTx=" + afterTx +
                ", txTimeout=" + txTimeout +
                ", defaultTz=" + defaultTz +
                ", explain=" + explain +
                ", keyFn=" + keyFn +
                '}';
    }
}