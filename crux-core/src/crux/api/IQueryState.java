package crux.api;

import clojure.lang.Keyword;

import java.util.Date;
import java.util.Map;

public interface IQueryState {
    enum QueryStatus {
        IN_PROGRESS, COMPLETED, FAILED
    }

    interface IQueryError {
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