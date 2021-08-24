package crux.api;

import java.util.Date;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

@SuppressWarnings("unused")
public final class HistoryOptions {
    public enum SortOrder {
        ASC("asc"),
        DESC("desc");

        private final Keyword keyword;

        SortOrder(String keyName) {
            this.keyword = Keyword.intern(keyName);
        }

        public Keyword getKeyword() {
            return keyword;
        }
    }

    private static final Keyword SORT_ORDER = Keyword.intern("sort-order");
    private static final Keyword WITH_CORRECTIONS = Keyword.intern("with-corrections?");
    private static final Keyword WITH_DOCS = Keyword.intern("with-docs?");
    private static final Keyword START_VALID_TIME = Keyword.intern("start-valid-time");
    private static final Keyword START_TX = Keyword.intern("start-tx");
    private static final Keyword END_VALID_TIME = Keyword.intern("end-valid-time");
    private static final Keyword END_TX = Keyword.intern("end-tx");

    private SortOrder sortOrder;
    private boolean withCorrections = false;
    private boolean withDocs = false;
    private Date startValidTime = null;
    private TransactionInstant startTransaction = null;
    private Date endValidTime = null;
    private TransactionInstant endTransaction = null;

    private HistoryOptions(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public static HistoryOptions create(SortOrder sortOrder) {
        return new HistoryOptions(sortOrder);
    }

    public HistoryOptions sortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
        return this;
    }

    /**
     * Specifies whether to return bitemporal corrections in the history response.
     *
     * If this is set to `true`, corrections will be returned within the
     * sequence, sorted first by valid-time, then tx-id.
     */
    public HistoryOptions withCorrections(boolean withCorrections) {
        this.withCorrections = withCorrections;
        return this;
    }

    /**
     * Specifies whether to return documents in the history response.
     *
     * If this is set to `true`, documents will be included under the
     * `:xt/doc` key.
     */
    public HistoryOptions withDocs(boolean withDocs) {
        this.withDocs = withDocs;
        return this;
    }

    /**
     * Sets the starting valid time.
     *
     * The history response will include entries starting at this valid time (inclusive).
     */
    public HistoryOptions startValidTime(Date startValidTime) {
        this.startValidTime = startValidTime;
        return this;
    }

    /**
     * Sets the starting transaction.
     *
     * The history response will include entries starting at this transaction (inclusive).
     */
    public HistoryOptions startTransaction(TransactionInstant startTransaction) {
        this.startTransaction = startTransaction;
        return this;
    }

    /**
     * Sets the starting transaction time.
     *
     * The history response will include entries starting at this transaction (inclusive).
     */
    public HistoryOptions startTransactionTime(Date startTransactionTime) {
        this.startTransaction = TransactionInstant.factory(startTransactionTime);
        return this;
    }

    /**
     * Sets the end valid time.
     *
     * The history response will include entries up to this valid time (exclusive).
     */
    public HistoryOptions endValidTime(Date endValidTime) {
        this.endValidTime = endValidTime;
        return this;
    }

    /**
     * Sets the ending transaction.
     *
     * The history response will include entries up to this transaction (exclusive).
     */
    public HistoryOptions endTransaction(TransactionInstant endTransaction) {
        this.endTransaction = endTransaction;
        return this;
    }

    /**
     * Sets the ending transaction time.
     *
     * The history response will include entries up to this transaction (exclusive).
     */
    public HistoryOptions endTransactionTime(Date endTransactionTime) {
        this.endTransaction = TransactionInstant.factory(endTransactionTime);
        return this;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public boolean isWithCorrections() {
        return withCorrections;
    }

    public boolean isWithDocs() {
        return withDocs;
    }

    public Date getStartValidTime() {
        return startValidTime;
    }

    public TransactionInstant getStartTransaction() {
        return startTransaction;
    }

    public Date getEndValidTime() {
        return endValidTime;
    }

    public TransactionInstant getEndTransaction() {
        return endTransaction;
    }

    public Keyword getSortOrderKey() {
        return sortOrder.getKeyword();
    }

    /**
     * Not intended for public use, may be removed.
     */
    public IPersistentMap toMap() {
        IPersistentMap ret = PersistentArrayMap.EMPTY
                .assoc(SORT_ORDER, sortOrder.keyword)
                .assoc(WITH_CORRECTIONS, withCorrections)
                .assoc(WITH_DOCS, withDocs)
                .assoc(START_VALID_TIME, startValidTime)
                .assoc(END_VALID_TIME, endValidTime);

        if (startTransaction != null) {
            ret = ret.assoc(START_TX, startTransaction.toMap());
        }

        if (endTransaction != null) {
            ret = ret.assoc(END_TX, endTransaction.toMap());
        }

        return ret;
    }
}
