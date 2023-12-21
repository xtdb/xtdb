package xtdb.query;

import java.util.Objects;

public class QueryRequest {
    private Query query;
    private QueryOpts queryOpts;

    public QueryRequest(Query query, QueryOpts queryOpts) {
        this.query = query;
        this.queryOpts = queryOpts;
    }

    public Query query() {
        return query;
    }

    public QueryOpts queryOpts() {
        return queryOpts;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRequest queryRequest = (QueryRequest) o;
        return Objects.equals(query, queryRequest.query) &&
               Objects.equals(queryOpts , queryRequest.queryOpts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, queryOpts);
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
                "query=" + query +
                ", queryOpts=" + queryOpts +
                '}';
    }
}
