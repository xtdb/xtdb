package crux.api;

import clojure.lang.Keyword;

import java.util.Date;
import java.util.Map;

public class QueryState {
    public enum QueryStatus {
        IN_PROGRESS, COMPLETED, FAILED
    }

    public static class QueryError {
        public final String errorClass;
        public final String errorMessage;

        public QueryError(String errorClass, String errorMessage) {
            this.errorClass = errorClass;
            this.errorMessage = errorMessage;
        }
    }

    public final Object queryId;
    public final Date startedAt;
    public final Date finishedAt;
    public final QueryStatus status;
    public final Map<Keyword, ?> query;
    public final QueryError error;

    public QueryState(Object queryId, Date startedAt, Date finishedAt, QueryStatus status, Map<Keyword, ?> query, QueryError error) {
        this.queryId = queryId;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.status = status;
        this.query = query;
        this.error = error;
    }
}
