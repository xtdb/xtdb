package crux.api;

import clojure.lang.ILookup;
import clojure.lang.Keyword;

import java.util.Date;
import java.util.Map;

@SuppressWarnings("unused")
public interface IQueryState extends ILookup {
    enum QueryStatus {
        IN_PROGRESS, COMPLETED, FAILED
    }

    interface IQueryError extends ILookup {
        String getErrorClass();
        String getErrorMessage();
    }

    Object getQueryId();
    Date getStartedAt();
    Date getFinishedAt();
    IQueryState.QueryStatus getStatus();
    Map<Keyword, ?> getQuery();
    IQueryState.IQueryError getError();
}