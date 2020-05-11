package crux.api;

import java.util.Date;

public class HistoryOptions {
    public enum SortOrder {
        ASC, DESC;
    }

    public final SortOrder sortOrder;

    /**
     * Specifies whether to return bitemporal corrections in the history response.
     *
     * If this is set to `true`, corrections will be returned within the
     * sequence, sorted first by valid-time, then transaction-time.
     */
    public final boolean withCorrections;

    /**
     * Specifies whether to return documents in the history response.
     *
     * If this is set to `true`, documents will be included under the
     * `:crux.db/doc` key.
     */
    public final boolean withDocs;

    /**
     * Sets the starting valid time.
     *
     * The history response will include entries starting at this valid time (inclusive).
     */
    public final Date startValidTime;

    /**
     * Sets the starting transaction time.
     *
     * The history response will include entries starting at this transaction time (inclusive).
     */
    public final Date startTransactionTime;

    /**
     * Sets the end valid time.
     *
     * The history response will include entries up to this valid time (exclusive).
     */
    public final Date endValidTime;

    /**
     * Sets the end transaction time.
     *
     * The history response will include entries up to this transaction time (exclusive).
     */
    public final Date endTransactionTime;

    public HistoryOptions(SortOrder sortOrder) {
        this(sortOrder, false, true, null, null, null, null);
    }

    public HistoryOptions(SortOrder sortOrder, boolean withCorrections, boolean withDocs,
                          Date startValidTime, Date startTransactionTime,
                          Date endValidTime, Date endTransactionTime) {
        this.sortOrder = sortOrder;
        this.withCorrections = withCorrections;
        this.withDocs = withDocs;
        this.startValidTime = startValidTime;
        this.startTransactionTime = startTransactionTime;
        this.endValidTime = endValidTime;
        this.endTransactionTime = endTransactionTime;
    }

    public HistoryOptions withCorrections(boolean withCorrections) {
        return new HistoryOptions(sortOrder, withCorrections, withDocs, startValidTime, startTransactionTime, endValidTime, endTransactionTime);
    }

    public HistoryOptions withDocs(boolean withDocs) {
        return new HistoryOptions(sortOrder, withCorrections, withDocs, startValidTime, startTransactionTime, endValidTime, endTransactionTime);
    }

    public HistoryOptions startValidTime(Date startValidTime) {
        return new HistoryOptions(sortOrder, withCorrections, withDocs, startValidTime, startTransactionTime, endValidTime, endTransactionTime);
    }

    public HistoryOptions startTransactionTime(Date startTransactionTime) {
        return new HistoryOptions(sortOrder, withCorrections, withDocs, startValidTime, startTransactionTime, endValidTime, endTransactionTime);
    }

    public HistoryOptions endValidTime(Date endValidTime) {
        return new HistoryOptions(sortOrder, withCorrections, withDocs, startValidTime, startTransactionTime, endValidTime, endTransactionTime);
    }

    public HistoryOptions endTransactionTime(Date endTransactionTime) {
        return new HistoryOptions(sortOrder, withCorrections, withDocs, startValidTime, startTransactionTime, endValidTime, endTransactionTime);
    }
}
