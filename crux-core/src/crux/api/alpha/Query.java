package crux.api.alpha;

import clojure.lang.*;

import java.util.Arrays;
import java.util.List;

import static crux.api.alpha.Util.keyword;

// Version of Query which reads strings.

public class Query {
    private static final Keyword FIND = keyword("find");
    private static final Keyword WHERE = keyword("where");
    private static final Keyword ARGS = keyword("args");
    private static final Keyword RULES = keyword("rules");
    private static final Keyword OFFSET = keyword("offset");
    private static final Keyword LIMIT = keyword("limit");
    private static final Keyword ORDER_BY = keyword("order-by");
    private static final Keyword TIMEOUT = keyword("timeout");
    private static final Keyword FULL_RESULTS = keyword("full-results?");

    private final PersistentVector findClause;
    private final PersistentVector whereClause;
    private final PersistentVector args;
    private final PersistentVector rules;
    private final Long offset;
    private final Long limit;
    private final PersistentVector orderBy;
    private final Long timeout;
    private final Boolean fullResults;

    private Query(PersistentVector findClause, PersistentVector whereClause, PersistentVector args,
                  PersistentVector rules, Long offset, Long limit, PersistentVector orderBy,
                  Long timeout, Boolean fullResults) {
        this.findClause = findClause;
        this.whereClause = whereClause;
        this.args = args;
        this.rules = rules;
        this.offset = offset;
        this.limit = limit;
        this.orderBy = orderBy;
        this.timeout = timeout;
        this.fullResults = fullResults;
    }

    public static class FindQuery {
        private final PersistentVector symbols;

        private FindQuery(PersistentVector symbols) {
            this.symbols = symbols;
        }

        public Query where(String whereClause) {
            PersistentVector whereVector = (PersistentVector) RT.readString(whereClause);
            return new Query(symbols, whereVector, null, null, null, null, null, null ,null);
        }
    }

    public static FindQuery find(String findClause) {
        PersistentVector findVector = (PersistentVector) RT.readString(findClause);
        return new FindQuery(findVector); }


    public static FindQuery find(List<Symbol> findVars) {
        PersistentVector findVector = PersistentVector.create();
        for (Symbol var : findVars) {
            findVector = findVector.cons(var);
        }
        return new FindQuery(findVector);
    }

    public static FindQuery find(Symbol... findVars) {
        return find(Arrays.asList(findVars));
    }

    public Query args(String args) {
        PersistentVector argsVector = (PersistentVector) RT.readString(args);
        return new Query(findClause, whereClause, argsVector, rules, offset, limit, orderBy, timeout, fullResults);
    }

    public Query rules(String rules) {
        PersistentVector rulesVector = (PersistentVector) RT.readString(rules);
        return new Query(findClause, whereClause, args, rulesVector, offset, limit, orderBy, timeout, fullResults);
    }

    public Query offset(Long offset) {
        return new Query(findClause, whereClause, args, rules, offset, limit, orderBy, timeout, fullResults);
    }

    public Query limit(Long limit) {
        return new Query(findClause, whereClause, args, rules, offset, limit, orderBy, timeout, fullResults);
    }

    public Query orderBy(String orderBy) {
        PersistentVector orderVector = (PersistentVector) RT.readString(orderBy);
        return new Query(findClause, whereClause, args, rules, offset, limit, orderVector, timeout, fullResults);
    }

    public Query timeout(Long timeout) {
        return new Query(findClause, whereClause, args, rules, offset, limit, orderBy, timeout, fullResults);
    }

    public Query fullResults(boolean fullResults) {
        return new Query(findClause, whereClause, args, rules, offset, limit, orderBy, timeout, fullResults);
    }

    @SuppressWarnings("unchecked")
    List<Symbol> findSymbols() {
        return findClause;
    }

    public IPersistentMap toEdn() {
        IPersistentMap queryMap = PersistentArrayMap.EMPTY;
        queryMap = queryMap.assoc(FIND, findClause);
        queryMap = queryMap.assoc(WHERE, whereClause);

        if(args!=null)
            queryMap = queryMap.assoc(ARGS, args);
        if(rules!=null)
            queryMap = queryMap.assoc(RULES, rules);
        if(offset!=null)
            queryMap = queryMap.assoc(OFFSET, offset);
        if(limit!=null)
            queryMap = queryMap.assoc(LIMIT, limit);
        if(orderBy!=null)
            queryMap = queryMap.assoc(ORDER_BY, orderBy);
        if(timeout!=null)
            queryMap = queryMap.assoc(TIMEOUT, timeout);
        if(fullResults!=null)
            queryMap = queryMap.assoc(FULL_RESULTS, fullResults);

        return queryMap;
    }
}
