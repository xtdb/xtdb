package xtdb.query;

import java.util.*;

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pipeline pipeline = (Pipeline) o;
            return Objects.equals(query, pipeline.query) && Objects.equals(tails, pipeline.tails);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, tails);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Unify unify = (Unify) o;
            return Objects.equals(clauses, unify.clauses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clauses);
        }
    }

    static Unify unify(List<UnifyClause> clauses) {
        return new Unify(clauses);
    }

    final class From implements Query, UnifyClause {
        public final String table;
        public final TemporalFilter forValidTime;
        public final TemporalFilter forSystemTime;
        public final List<OutSpec> bindings;
        public final boolean projectAllCols;

        private From(String table, TemporalFilter forValidTime, TemporalFilter forSystemTime, List<OutSpec> bindings, boolean projectAllCols) {
            this.table = table;
            this.forValidTime = forValidTime;
            this.forSystemTime = forSystemTime;
            this.bindings = unmodifiableList(bindings);
            this.projectAllCols = projectAllCols;
        }

        public From forValidTime(TemporalFilter forValidTime) {
            return new From(table, forValidTime, forSystemTime, bindings, projectAllCols);
        }

        public From forSystemTime(TemporalFilter forSystemTime) {
            return new From(table, forValidTime, forSystemTime, bindings, projectAllCols);
        }

        public From projectAllCols(boolean projectAllCols) {
            return new From(table, forValidTime, forSystemTime, bindings, projectAllCols);
        }

        public From binding(List<OutSpec> bindings) {
            return new From(table, forValidTime, forSystemTime, bindings, projectAllCols);
        }

        @Override
        public String toString() {
            Map<String, TemporalFilter> temporalFilters = null;

            if (forValidTime != null || forSystemTime != null) {
                temporalFilters = new HashMap<>();
                if (forValidTime != null) temporalFilters.put("forValidTime", forValidTime);
                if (forSystemTime != null) temporalFilters.put("forSystemTime", forSystemTime);
            }

            return String.format("(from %s [%s %s])", stringifyOpts(table, temporalFilters), (projectAllCols ? "*" : ""), stringifyList(bindings));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            From from = (From) o;
            return projectAllCols == from.projectAllCols && Objects.equals(table, from.table) && Objects.equals(forValidTime, from.forValidTime) && Objects.equals(forSystemTime, from.forSystemTime) && Objects.equals(bindings, from.bindings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, forValidTime, forSystemTime, bindings, projectAllCols);
        }
    }

    static From from(String table) {
        return new From(table, null, null, null, false);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Where where = (Where) o;
            return Objects.equals(preds, where.preds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(preds);
        }
    }

    static Where where(List<Expr> preds) {
        return new Where(preds);
    }

    final class With implements UnifyClause {
        public final List<VarSpec> vars;

        private With(List<VarSpec> vars) {
            this.vars = unmodifiableList(vars);
        }

        @Override
        public String toString() {
            return String.format("(with %s)", stringifyList(vars));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            With with = (With) o;
            return Objects.equals(vars, with.vars);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vars);
        }
    }

    static With with(List<VarSpec> vars) {
        return new With(vars);
    }

    final class WithCols implements QueryTail {
        public final List<ColSpec> cols;

        private WithCols(List<ColSpec> cols) {
            this.cols = unmodifiableList(cols);
        }

        @Override
        public String toString() {
            return String.format("(with %s)", stringifyList(cols));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WithCols withCols = (WithCols) o;
            return Objects.equals(cols, withCols.cols);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cols);
        }
    }

    static WithCols withCols(List<ColSpec> cols) {
        return new WithCols(cols);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Without without = (Without) o;
            return Objects.equals(cols, without.cols);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cols);
        }
    }

    static Without without(List<String> cols) {
        return new Without(cols);
    }

    final class Return implements QueryTail {
        public final List<ColSpec> cols;

        private Return(List<ColSpec> cols) {
            this.cols = unmodifiableList(cols);
        }

        @Override
        public String toString() {
            return String.format("(return %s)", stringifyList(cols));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Return returnObj = (Return) o;
            return Objects.equals(cols, returnObj.cols);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cols);
        }
    }

    static Return returning(List<ColSpec> cols) {
        return new Return(cols);
    }

    final class Call implements UnifyClause {
        public final String ruleName;
        public final List<Expr> args;
        public final List<OutSpec> bindings;

        private Call(String ruleName, List<Expr> args, List<OutSpec> bindings) {
            this.ruleName = ruleName;
            this.args = unmodifiableList(args);
            this.bindings = bindings;
        }

        public Call binding(List<OutSpec> bindings) {
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

    abstract interface IJoin extends UnifyClause {
        IJoin binding(List<OutSpec> bindings);
    }

    final class Join implements IJoin {
        public final Query query;
        public final List<ArgSpec> args;
        public final List<OutSpec> bindings;

        private Join(Query query, List<ArgSpec> args, List<OutSpec> bindings) {
            this.query = query;
            this.args = unmodifiableList(args);
            this.bindings = unmodifiableList(bindings);
        }

        public Join binding(List<OutSpec> bindings) {
            return new Join(query, args, bindings);
        }

        @Override
        public String toString() {
            return stringifySeq("join", stringifyArgs(query, args), stringifyList(bindings));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Join join = (Join) o;
            return Objects.equals(query, join.query) && Objects.equals(args, join.args) && Objects.equals(bindings, join.bindings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, args, bindings);
        }
    }

    static Join join(Query query, List<ArgSpec> args) {
        return new Join(query, args, null);
    }

    final class LeftJoin implements IJoin {
        public final Query query;
        public final List<ArgSpec> args;
        public final List<OutSpec> bindings;

        private LeftJoin(Query query, List<ArgSpec> args, List<OutSpec> bindings) {
            this.query = query;
            this.args = unmodifiableList(args);
            this.bindings = unmodifiableList(bindings);
        }

        public LeftJoin binding(List<OutSpec> bindings) {
            return new LeftJoin(query, args, bindings);
        }

        @Override
        public String toString() {
            return stringifySeq("join", stringifyArgs(query, args), stringifyList(bindings));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LeftJoin leftJoin = (LeftJoin) o;
            return Objects.equals(query, leftJoin.query) && Objects.equals(args, leftJoin.args) && Objects.equals(bindings, leftJoin.bindings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, args, bindings);
        }
    }

    static LeftJoin leftJoin(Query query, List<ArgSpec> args) {
        return new LeftJoin(query, args, null);
    }

    final class Aggregate implements QueryTail {
        public final List<ColSpec> cols;

        private Aggregate(List<ColSpec> cols) {
            this.cols = unmodifiableList(cols);
        }

        @Override
        public String toString() {
            return stringifySeq("aggregate", stringifyList(cols));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Aggregate aggregate = (Aggregate) o;
            return Objects.equals(cols, aggregate.cols);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cols);
        }
    }

    static Aggregate aggregate(List<ColSpec> cols) {
        return new Aggregate(cols);
    }

    enum OrderDirection {
        ASC, DESC
    }

    enum OrderNulls {
        FIRST, LAST
    }

    final class OrderSpec {
        public final Expr expr;
        public final OrderDirection direction;
        public final OrderNulls nulls;

        private OrderSpec(Expr expr, OrderDirection direction, OrderNulls nulls) {
            this.expr = expr;
            this.direction = direction;
            this.nulls = nulls;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s", expr, direction, nulls);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderSpec orderSpec = (OrderSpec) o;
            return Objects.equals(expr, orderSpec.expr) &&
                direction == orderSpec.direction &&
                nulls == orderSpec.nulls;
        }

        @Override
        public int hashCode() {
            return Objects.hash(expr, direction, nulls);
        }
    }

    static OrderSpec orderSpec(Expr expr, OrderDirection direction, OrderNulls nulls) {
        return new OrderSpec(expr, direction, nulls);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderBy orderBy = (OrderBy) o;
            return Objects.equals(orderSpecs, orderBy.orderSpecs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderSpecs);
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
        public final long length;

        private Limit(long length) {
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format("(%s %s)", "limit", length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Limit limit = (Limit) o;
            return length == limit.length;
        }

        @Override
        public int hashCode() {
            return Objects.hash(length);
        }
    }

    static Limit limit(long length) {
        return new Limit(length);
    }

    final class Offset implements QueryTail {
        public final long length;

        private Offset(long length) {
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format("(%s %s)", "offset", length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Offset offset = (Offset) o;
            return length == offset.length;
        }

        @Override
        public int hashCode() {
            return Objects.hash(length);
        }
    }

    static Offset offset(long length) {
        return new Offset(length);

    }

    abstract class Relation implements Query, UnifyClause {}
    final class DocsRelation extends Relation {
        public final List<Map<String, Expr>> documents;
        public final List<OutSpec> bindings;

        private DocsRelation(List<Map<String, Expr>> documents, List<OutSpec> bindings) {
            this.documents = documents;
            this.bindings = bindings;
        }

        public DocsRelation bindings(List<OutSpec> bindings) { return new DocsRelation(documents, bindings); }

        @Override
        public String toString() {
            return stringifySeq("rel", stringifyList(documents), stringifyList(bindings));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocsRelation that = (DocsRelation) o;
            return Objects.equals(documents, that.documents) && Objects.equals(bindings, that.bindings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(documents, bindings);
        }
    }

    final class ParamRelation extends Relation {
        public final Expr.Param param;
        public final List<OutSpec> bindings;

        private ParamRelation(Expr.Param param, List<OutSpec> bindings) {
            this.bindings = bindings;
            this.param = param;
        }

        @Override
        public String toString() {
            return stringifySeq("rel", param.toString(), stringifyList(bindings));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParamRelation that = (ParamRelation) o;
            return Objects.equals(param, that.param) && Objects.equals(bindings, that.bindings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(param, bindings);
        }
    }

    static DocsRelation relation(List<Map<String, Expr>> documents, List<OutSpec> bindings) {
        return new DocsRelation(documents, bindings);
    }

    static ParamRelation relation(Expr.Param param, List<OutSpec> bindings) {
        return new ParamRelation(param, bindings);
    }

    final class UnnestVar implements UnifyClause {
        public final VarSpec var;

        private UnnestVar(VarSpec var) {
            this.var = var;
        }

        @Override
        public String toString() {
            return String.format("(unnest %s)", var);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnnestVar unnestVar = (UnnestVar) o;
            return Objects.equals(var, unnestVar.var);
        }

        @Override
        public int hashCode() {
            return Objects.hash(var);
        }
    }

    static UnnestVar unnestVar(VarSpec var) {
        return new UnnestVar(var);
    }

    final class UnnestCol implements QueryTail {
        public final ColSpec col;

        private UnnestCol(ColSpec col) {
            this.col = col;
        }

        @Override
        public String toString() {
            return String.format("(unnest %s)", col);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnnestCol unnestCol = (UnnestCol) o;
            return Objects.equals(col, unnestCol.col);
        }

        @Override
        public int hashCode() {
            return Objects.hash(col);
        }
    }

    static UnnestCol unnestCol(ColSpec col) {
        return new UnnestCol(col);
    }
}
