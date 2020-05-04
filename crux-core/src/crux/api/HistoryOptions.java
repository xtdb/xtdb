package crux.api;

import java.util.Date;

public class HistoryOptions {
    public enum SortOrder {
        ASC, DESC;
    }

    public final SortOrder sortOrder;
    public final boolean withCorrections;
    public final boolean withDocs;

    public final Date startValidTime;
    public final Date startTransactionTime;
    public final Date endValidTime;
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
