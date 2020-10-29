package crux.api;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.ILookup;
import clojure.java.api.Clojure;

public interface HistoryOptions extends ILookup {
    public enum SortOrder {
        ASC, DESC;
    }

    class Builder {
        private static final IFn REQUIRING_RESOLVE =
            Clojure.var("clojure.core", "requiring-resolve");

        static final IFn TO_HISTORY_OPTIONS =
            (IFn) REQUIRING_RESOLVE.invoke(Clojure.read("crux.history-options/->history-options"));
    }

    public static HistoryOptions create(SortOrder sortOrder) {
        return (HistoryOptions) Builder.TO_HISTORY_OPTIONS.invoke(sortOrder);
    }

    public HistoryOptions sortOrder(SortOrder sortOrder);

    /**
     * Specifies whether to return bitemporal corrections in the history response.
     *
     * If this is set to `true`, corrections will be returned within the
     * sequence, sorted first by valid-time, then transaction-time.
     */
    public HistoryOptions withCorrections(boolean withCorrections);

    /**
     * Specifies whether to return documents in the history response.
     *
     * If this is set to `true`, documents will be included under the
     * `:crux.db/doc` key.
     */
    public HistoryOptions withDocs(boolean withDocs);

    /**
     * Sets the starting valid time.
     *
     * The history response will include entries starting at this valid time (inclusive).
     */
    public HistoryOptions startValidTime(Date validTime);

    /**
     * Sets the starting transaction.
     *
     * The history response will include entries starting at this transaction (inclusive).
     */
    public HistoryOptions startTransaction(Map<Keyword, ?> startTransaction);

    /**
     * Sets the starting transaction time.
     *
     * The history response will include entries starting at this transaction (inclusive).
     */
    public HistoryOptions startTransactionTime(Date startTransactionTime);

    /**
     * Sets the end valid time.
     *
     * The history response will include entries up to this valid time (exclusive).
     */
    public HistoryOptions endValidTime(Date endValidTime);

    /**
     * Sets the ending transaction.
     *
     * The history response will include entries up to this transaction (exclusive).
     */
    public HistoryOptions endTransaction(Map<Keyword, ?> endTransaction);

    /**
     * Sets the ending transaction time.
     *
     * The history response will include entries up to this transaction (exclusive).
     */
    public HistoryOptions endTransactionTime(Date endTransactionTime);
}
