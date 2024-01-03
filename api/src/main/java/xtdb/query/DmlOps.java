package xtdb.query;

import xtdb.query.TemporalFilter.TemporalExtents;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static xtdb.query.Query.*;
import static xtdb.query.QueryUtil.*;

public interface DmlOps {
    final class Insert implements DmlOps {
        public final String table;
        public final Query query;

        private Insert(String table, Query query) {
            this.table = table;
            this.query = query;
        }

        @Override
        public String toString() {
            return String.format("(insert %s %s)", table, query);
        }
    }

    static Insert insert(String table, Query query) {
        return new Insert(table, query);
    }

    final class Update implements DmlOps {
        public final String table;
        public final List<Binding> setSpecs;
        public final TemporalExtents forValidTime;
        public final List<Binding> bindSpecs;
        public final List<? extends UnifyClause> unifyClauses;

        private Update(String table, List<Binding> setSpecs, TemporalExtents forValidTime, List<Binding> bindSpecs,
                       List<? extends UnifyClause> unifyClauses) {
            this.table = table;
            this.setSpecs = setSpecs;
            this.forValidTime = forValidTime;
            this.bindSpecs = bindSpecs;
            this.unifyClauses = unifyClauses;
        }

        public Update forValidTime(TemporalExtents forValidTime) {
            return new Update(table, setSpecs, forValidTime, bindSpecs, unifyClauses);
        }

        public Update binding(List<Binding> bindSpecs) {
            return new Update(table, setSpecs, forValidTime, bindSpecs, unifyClauses);
        }

        public Update unify(List<? extends UnifyClause> unifyClauses) {
            return new Update(table, setSpecs, forValidTime, bindSpecs, unmodifiableList(unifyClauses));
        }

        @Override
        public String toString() {
            Map<String, Object> opts = new HashMap<>();

            if (bindSpecs != null && !bindSpecs.isEmpty()) opts.put("bind", bindSpecs);
            opts.put("set", setSpecs);

            if (forValidTime != null) {
                opts.put("forValidTime", forValidTime);
            }

            return stringifySeq("update", stringifyOpts(table, opts), stringifyList(unifyClauses));
        }
    }

    static Update update(String table, List<Binding> setSpecs) {
        return new Update(table, setSpecs, null, null, null);
    }

    final class Delete implements DmlOps {
        public final String table;
        public final TemporalExtents forValidTime;
        public final List<Binding> bindSpecs;
        public final List<? extends UnifyClause> unifyClauses;

        private Delete(String table, TemporalExtents forValidTime, List<Binding> bindSpecs,
                       List<? extends UnifyClause> unifyClauses) {
            this.table = table;
            this.forValidTime = forValidTime;
            this.bindSpecs = bindSpecs;
            this.unifyClauses = unifyClauses;
        }

        public Delete forValidTime(TemporalExtents forValidTime) {
            return new Delete(table, forValidTime, bindSpecs, unifyClauses);
        }

        public Delete binding(List<Binding> bindSpecs) {
            return new Delete(table, forValidTime, bindSpecs, unifyClauses);
        }

        public Delete unify(List<? extends UnifyClause> unifyClauses) {
            return new Delete(table, forValidTime, bindSpecs, unmodifiableList(unifyClauses));
        }

        @Override
        public String toString() {
            Map<String, Object> opts = new HashMap<>();

            if (bindSpecs != null && !bindSpecs.isEmpty()) opts.put("bind", bindSpecs);

            if (forValidTime != null) {
                opts.put("forValidTime", forValidTime);
            }

            return stringifySeq("delete", stringifyOpts(table, opts), stringifyList(unifyClauses));
        }
    }

    static Delete delete(String table) {
        return new Delete(table, null, null, null);
    }

    final class Erase implements DmlOps {
        public final String table;
        public final List<Binding> bindSpecs;
        public final List<? extends UnifyClause> unifyClauses;

        private Erase(String table, List<Binding> bindSpecs,
                      List<? extends UnifyClause> unifyClauses) {
            this.table = table;
            this.bindSpecs = bindSpecs;
            this.unifyClauses = unifyClauses;
        }

        public Erase binding(List<Binding> bindSpecs) {
            return new Erase(table, bindSpecs, unifyClauses);
        }

        public Erase unify(List<? extends UnifyClause> unifyClauses) {
            return new Erase(table, bindSpecs, unmodifiableList(unifyClauses));
        }

        @Override
        public String toString() {
            Map<String, Object> opts = new HashMap<>();

            if (bindSpecs != null && !bindSpecs.isEmpty()) opts.put("bind", bindSpecs);

            return stringifySeq("erase", stringifyOpts(table, opts), stringifyList(unifyClauses));
        }
    }

    static Erase erase(String table) {
        return new Erase(table, null, null);
    }

    final class AssertExists implements DmlOps {
        public final Query query;

        private AssertExists(Query query) {
            this.query = query;
        }

        @Override
        public String toString() {
            return String.format("(assert-exists %s)", query);
        }
    }

    static AssertExists assertExists(Query query) {
        return new AssertExists(query);
    }

    final class AssertNotExists implements DmlOps {
        public final Query query;

        private AssertNotExists(Query query) {
            this.query = query;
        }

        @Override
        public String toString() {
            return String.format("(assert-not-exists %s)", query);
        }
    }

    static AssertNotExists assertNotExists(Query query) {
        return new AssertNotExists(query);
    }
}
