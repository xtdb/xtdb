package crux.api.v2;

import clojure.lang.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// Version of Query which reads strings.

public class Query {
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
        private final PersistentVector logicVars;

        private FindQuery(PersistentVector logicVars) {
            this.logicVars = logicVars;
        }

        public Query where(String whereClause) {
            PersistentVector whereVector = (PersistentVector) RT.readString(whereClause);
            return new Query(logicVars, whereVector, null, null, null, null, null, null ,null);
        }
    }

    public static FindQuery find(String findClause) {
        PersistentVector findVector = (PersistentVector) RT.readString(findClause);
        return new FindQuery(findVector); }


    public static FindQuery find(List<LogicVar> findVars) {
        PersistentVector findVector = PersistentVector.create();
        for (LogicVar var : findVars) {
            findVector = findVector.cons(var.toEdn());
        }
        return new FindQuery(findVector);
    }

    public static FindQuery find(LogicVar... findVars) {
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

    public List<LogicVar> findLogicVars() {
        List<LogicVar> logicVarList =
            new ArrayList<Symbol>(findClause).stream()
            .map(sym -> LogicVar.logicVar(sym))
            .collect(Collectors.toList());

        return logicVarList;
    }

    public IPersistentMap toEdn() {
        IPersistentMap queryMap = PersistentArrayMap.EMPTY;
        queryMap = queryMap.assoc(Keyword.intern("find"), findClause);
        queryMap = queryMap.assoc(Keyword.intern("where"), whereClause);

        if(args!=null)
            queryMap = queryMap.assoc(Keyword.intern("args"), args);
        if(rules!=null)
            queryMap = queryMap.assoc(Keyword.intern("rules"), rules);
        if(offset!=null)
            queryMap = queryMap.assoc(Keyword.intern("offset"), offset);
        if(limit!=null)
            queryMap = queryMap.assoc(Keyword.intern("limit"), limit);
        if(orderBy!=null)
            queryMap = queryMap.assoc(Keyword.intern("order-by"), orderBy);
        if(timeout!=null)
            queryMap = queryMap.assoc(Keyword.intern("timeout"), timeout);
        if(fullResults!=null)
            queryMap = queryMap.assoc(Keyword.intern("full-results?"), fullResults);

        return queryMap;
    }
}
