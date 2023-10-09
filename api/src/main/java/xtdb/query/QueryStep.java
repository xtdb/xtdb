package xtdb.query;

import java.util.*;

import static xtdb.query.QueryStep.OrderDirection.ASC;
import static xtdb.query.QueryStep.OrderDirection.DESC;
import static xtdb.query.QueryUtil.*;

public interface QueryStep {

    final class Pipeline implements QueryStep {
        public final List<QueryStep> steps;

        private Pipeline(List<QueryStep> steps) {
            this.steps = unmodifiableList(steps);
        }

        @Override
        public String toString() {
            return stringifySeq("->", stringifyList(steps));
        }
    }

    static Pipeline pipeline(List<QueryStep> steps) {
        return new Pipeline(steps);
    }

    interface UnifyClause {
    }

    final class Unify implements QueryStep {
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

    final class BindingSpec {
        public final String attr;
        public final Expr expr;

        private BindingSpec(String attr, Expr expr) {
            this.attr = attr;
            this.expr = expr;
        }

        @Override
        public String toString() {
            return String.format("{%s %s}", attr, expr);
        }
    }

    static BindingSpec bindSpec(String attr, Expr expr) {
        return new BindingSpec(attr, expr);
    }

    final class From implements QueryStep, UnifyClause {
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

    final class Where implements QueryStep, UnifyClause {
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

    final class With implements QueryStep, UnifyClause {
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

    final class Without implements QueryStep {
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

    final class Return implements QueryStep {
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

    final class LeftJoin implements UnifyClause {
        public final QueryStep query;
        public final List<BindingSpec> params;
        public final List<BindingSpec> bindings;

        private LeftJoin(QueryStep query, List<BindingSpec> params, List<BindingSpec> bindings) {
            this.query = query;
            this.params = unmodifiableList(params);
            this.bindings = unmodifiableList(bindings);
        }

        @Override
        public String toString() {
            return stringifySeq("left-join", stringifyParams(query, params), stringifyList(bindings));
        }
    }

    static LeftJoin leftJoin(QueryStep query) {
        return new LeftJoin(query, null, null);
    }

    final class Aggregate implements QueryStep {
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

    final class OrderBy implements QueryStep {
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
}

