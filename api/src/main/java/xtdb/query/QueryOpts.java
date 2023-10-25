package xtdb.query;

import java.util.List;

import static xtdb.query.QueryUtil.*;

public final class QueryOpts {


    public final List<ArgSpec> args;

    public QueryOpts(List<ArgSpec> args) {
        this.args = unmodifiableList(args);
    }

    @Override

    public String toString() {
        return stringifyList(args);
    }

}
