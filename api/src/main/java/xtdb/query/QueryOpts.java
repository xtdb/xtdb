package xtdb.query;

import clojure.lang.*;
import xtdb.api.TransactionKey;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class QueryOpts implements ILookup, Seqable {
    private static final Keyword ARGS_KEY = Keyword.intern("args");
    private static final Keyword BASIS_KEY = Keyword.intern("basis");
    private static final Keyword AFTER_TX_KEY = Keyword.intern("after-tx");
    private static final Keyword TX_TIMEOUT_KEY = Keyword.intern("tx-timeout");
    private static final Keyword DEFAULT_TZ_KEY = Keyword.intern("default-tz");
    private static final Keyword EXPLAIN_KEY = Keyword.intern("explain?");
    private static final Keyword KEY_FN_KEY = Keyword.intern("key-fn");

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

    @Override
    public Object valAt(Object key) {
        return valAt(key, null);
    }

    @Override
    public Object valAt(Object key, Object notFound) {
        if (key == ARGS_KEY) {
            return args;
        } else if (key == BASIS_KEY) {
            return basis;
        } else if (key == AFTER_TX_KEY){
            return afterTx;
        } else if (key == TX_TIMEOUT_KEY) {
            return txTimeout;
        } else if (key == DEFAULT_TZ_KEY) {
            return defaultTz;
        } else if (key == EXPLAIN_KEY) {
            return explain;
        } else if (key == KEY_FN_KEY) {
           return keyFn;
        } else {
            return notFound;
        }
    }

    @Override
    public ISeq seq() {
        List<Object> seqList = new ArrayList<>();
        if (args != null) {
            seqList.add(MapEntry.create(ARGS_KEY, args));
        }
        if (basis != null) {
            seqList.add(MapEntry.create(BASIS_KEY, basis));
        }
        if (afterTx != null) {
            seqList.add(MapEntry.create(AFTER_TX_KEY, afterTx));
        }
        if (txTimeout != null) {
            seqList.add(MapEntry.create(TX_TIMEOUT_KEY, txTimeout));
        }
        if (defaultTz != null) {
            seqList.add(MapEntry.create(DEFAULT_TZ_KEY, defaultTz));
        }
        if (EXPLAIN_KEY != null) {
            seqList.add(MapEntry.create(EXPLAIN_KEY, explain));
        }
        if (keyFn != null) {
            seqList.add(MapEntry.create(KEY_FN_KEY, keyFn));
        }
        return PersistentList.create(seqList).seq();
    }
}