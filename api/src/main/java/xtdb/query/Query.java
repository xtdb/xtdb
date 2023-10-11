package xtdb.query;

import java.util.*;

import static xtdb.query.Query.OrderDirection.ASC;
import static xtdb.query.Query.OrderDirection.DESC;
import static xtdb.query.QueryUtil.*;

public interface Query {

    interface QueryTail {
    }

    final class Pipeline implements Query {
        public final Query query;
        public final List<QueryTail> tails;

        private Pipeline(Query query, List<QueryTail> tails) {
            this.query = query;
            this.tails = unmodifiableList(tails);
        }

        @Override
        public String toString() {
            return stringifySeq("->", query, stringifyList(tails));
        }
    }

    static Pipeline pipeline(Query query, List<QueryTail> tails) {
        return new Pipeline(query, tails);
    }

    interface UnifyClause {
    }

    final class Unify implements Query {
        public final List<UnifyClause> clauses;

        private Unify(List<UnifyClause> clauses) {
            this.clauses = unmodifiableList(clauses);
        }

        @Override
        public String toString() {
            return stringifySeq("unify", stringifyList(clauses));
        }
    }

    static Unify unify(List<UnifyClause> clauses) {
        return new Unify(clauses);
    }

    final class From implements Query, UnifyClause {
        public final String table;
        public final TemporalFilter forValidTime;
        public final TemporalFilter forSystemTime;
        public final List<BindingSpec> bindSpecs;

        private From(String table, TemporalFilter forValidTime, TemporalFilter forSystemTime, List<BindingSpec> bindSpecs) {
            this.table = table;
            this.forValidTime = forValidTime;
            this.forSystemTime = forSystemTime;
            this.bindSpecs = unmodifiableList(bindSpecs);
        }

        public From forValidTime(TemporalFilter forValidTime) {
            return new From(table, forValidTime, forSystemTime, bindSpecs);
        }

        public From forSystemTime(TemporalFilter forSystemTime) {
            return new From(table, forValidTime, forSystemTime, bindSpecs);
        }

        public From binding(List<BindingSpec> bindSpecs) {
            return new From(table, forValidTime, forSystemTime, bindSpecs);
        }

        @Override
        public String toString() {
            Map<String, TemporalFilter> temporalFilters = null;

            if (forValidTime != null || forSystemTime != null) {
                temporalFilters = new HashMap<>();
                if (forValidTime != null) temporalFilters.put("forValidTime", forValidTime);
                if (forSystemTime != null) temporalFilters.put("forSystemTime", forSystemTime);
            }

            return String.format("(from %s %s)", stringifyOpts(table, temporalFilters), stringifyList(bindSpecs));
        }
    }

    static From from(String table) {
        return new From(table, null, null, null);
    }

    final class Where implements QueryTail, UnifyClause {
        public final List<Expr> preds;

        private Where(List<Expr> preds) {
            this.preds = unmodifiableList(preds);
        }

        @Override
        public String toString() {
            return stringifySeq("where", stringifyList(preds));
        }
    }

    static Where where(List<Expr> preds) {
        return new Where(preds);
    }

    final class With implements QueryTail, UnifyClause {
        public final List<BindingSpec> cols;

        private With(List<BindingSpec> cols) {
            this.cols = unmodifiableList(cols);
        }

        @Override
        public String toString() {
            return String.format("(with %s)", stringifyList(cols));
        }
    }

    static With with(List<BindingSpec> cols) {
        return new With(cols);
    }

    final class Without implements QueryTail {
        public final List<String> cols;

        private Without(List<String> cols) {
            this.cols = unmodifiableList(cols);
        }

        @Override
        public String toString() {
            return stringifySeq("without", stringifyList(cols));
        }
    }

    static Without without(List<String> cols) {
        return new Without(cols);
    }

    final class Return implements QueryTail {
        public final List<BindingSpec> cols;

        private Return(List<BindingSpec> cols) {
            this.cols = unmodifiableList(cols);
        }

        @Override
        public String toString() {
            return String.format("(return %s)", stringifyList(cols));
        }
    }

    static Return ret(List<BindingSpec> cols) {
        return new Return(cols);
    }

    final class Call implements UnifyClause {
        public final String ruleName;
        public final List<Expr> args;
        public final List<BindingSpec> bindings;

        private Call(String ruleName, List<Expr> args, List<BindingSpec> bindings) {
            this.ruleName = ruleName;
            this.args = unmodifiableList(args);
            this.bindings = bindings;
        }

        public Call binding(List<BindingSpec> bindings) {
            return new Call(ruleName, args, unmodifiableList(bindings));
        }

        @Override
        public String toString() {
            return stringifySeq("call", stringifyArgs(ruleName, args), stringifyList(bindings));
        }
    }

    static Call call(String ruleName, List<Expr> args) {
        return new Call(ruleName, args, null);
    }

    final class Join implements UnifyClause {
        public final Query query;
        public final List<BindingSpec> args;
        public final List<BindingSpec> bindings;

        private Join(Query query, List<BindingSpec> args, List<BindingSpec> bindings) {
            this.query = query;
            this.args = unmodifiableList(args);
            this.bindings = unmodifiableList(bindings);
        }

        public Join binding(List<BindingSpec> bindings) {
            return new Join(query, args, bindings);
        }

        @Override
        public String toString() {
            return stringifySeq("join", stringifyArgs(query, args), stringifyList(bindings));
        }
    }

    static Join join(Query query, List<BindingSpec> args) {
        return new Join(query, args, null);
    }

    final class LeftJoin implements UnifyClause {
        public final Query query;
        public final List<BindingSpec> args;
        public final List<BindingSpec> bindings;

        private LeftJoin(Query query, List<BindingSpec> args, List<BindingSpec> bindings) {
            this.query = query;
            this.args = unmodifiableList(args);
            this.bindings = unmodifiableList(bindings);
        }

        public LeftJoin binding(List<BindingSpec> bindings) {
            return new LeftJoin(query, args, bindings);
        }

        @Override
        public String toString() {
            return stringifySeq("left-join", stringifyArgs(query, args), stringifyList(bindings));
        }
    }

    static LeftJoin leftJoin(Query query, List<BindingSpec> args) {
        return new LeftJoin(query, args, null);
    }

    final class Aggregate implements QueryTail {
        public final List<BindingSpec> cols;

        private Aggregate(List<BindingSpec> cols) {
            this.cols = unmodifiableList(cols);
        }

        @Override
        public String toString() {
            return stringifySeq("aggregate", stringifyList(cols));
        }
    }

    static Aggregate aggregate(List<BindingSpec> cols) {
        return new Aggregate(cols);
    }

    enum OrderDirection {
        ASC, DESC
    }

    final class OrderSpec {
        public final Expr expr;
        public final OrderDirection direction;

        private OrderSpec(Expr expr, OrderDirection direction) {
            this.expr = expr;
            this.direction = direction;
        }

        @Override
        public String toString() {
            return String.format("%s %s", expr, direction);
        }
    }

    static OrderSpec asc(Expr expr) {
        return new OrderSpec(expr, ASC);
    }

    static OrderSpec desc(Expr expr) {
        return new OrderSpec(expr, DESC);
    }

    final class OrderBy implements QueryTail {
        public final List<OrderSpec> orderSpecs;

        private OrderBy(List<OrderSpec> orderSpecs) {
            this.orderSpecs = unmodifiableList(orderSpecs);
        }

        @Override
        public String toString() {
            return String.format("(%s %s)", "order-by", stringifyList(orderSpecs));
        }
    }

    static OrderBy orderBy(List<OrderSpec> orderSpecs) {
        return new OrderBy(orderSpecs);
    }

    final class UnionAll implements Query {
        public final List<Query> queries;

        private UnionAll(List<Query> queries) {
            this.queries = unmodifiableList(queries);
        }

        @Override
        public String toString() {
            return stringifySeq("union-all", stringifyList(queries));
        }
    }

    static UnionAll unionAll(List<Query> queries) {
        return new UnionAll(queries);
    }

    final class Limit implements QueryTail {
        public final Long length;

        private Limit(Long length) {
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format("(%s %s)", "limit", length);
        }
    }

    static Limit limit(Long length) {
        return new Limit(length);
    }
}

