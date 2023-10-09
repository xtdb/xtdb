package xtdb.query;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface Expr {
    Bool TRUE = new Bool(true);
    Bool FALSE = new Bool(false);

    final class Bool implements Expr {

        public final boolean bool;

        private Bool(boolean bool) {
            this.bool = bool;
        }

        @Override
        public String toString() {
            return Boolean.toString(bool);
        }
    }

    final class Long implements Expr {
        public final long lng;

        private Long(long lng) {
            this.lng = lng;
        }

        @Override
        public String toString() {
            return java.lang.Long.toString(lng);
        }
    }

    static Long val(long l) {
        return new Long(l);
    }

    final class Double implements Expr {
        public final double dbl;

        private Double(double dbl) {
            this.dbl = dbl;
        }

        @Override
        public String toString() {
            return java.lang.Double.toString(dbl);
        }
    }

    static Double val(double d) {
        return new Double(d);
    }

    final class Obj implements Expr {
        private static final IFn PR_STR = Clojure.var("clojure.core/pr-str");
        public final Object obj;

        private Obj(Object obj) {
            this.obj = obj;
        }

        @Override
        public String toString() {
            return (String) PR_STR.invoke(obj);
        }
    }

    static Obj val(Object obj) {
        return new Obj(obj);
    }

    final class LogicVar implements Expr {
        public final String lv;

        private LogicVar(String lv) {
            this.lv = lv;
        }

        @Override
        public String toString() {
            return lv;
        }
    }

    static LogicVar lVar(String lv) {
        return new LogicVar(lv);
    }

    final class Call implements Expr {
        public final String f;
        public final List<Expr> args;

        private Call(String f, List<Expr> args) {
            this.f = f;
            this.args = Collections.unmodifiableList(args);
        }

        @Override
        public String toString() {
            String args = this.args == null || this.args.isEmpty() ? null : " " + this.args.stream().map(Object::toString).collect(Collectors.joining(" "));
            return String.format("(%s%s)", f, args);
        }
    }

    static Call call(String f, List<Expr> args) {
        return new Call(f, args);
    }

    final class Query implements Expr {
        public final QueryStep query;
        public final Map<String, Expr> params;

        private Query(QueryStep query, Map<String, Expr> params) {
            this.query = query;
            this.params = params != null ? Collections.unmodifiableMap(params) : null;
        }

        @Override
        public String toString() {
            String paramsStr = params != null ? params.entrySet().stream().map(e -> String.format("%s %s", e.getKey(), e.getValue())).collect(Collectors.joining(", ")) : null;
            return String.format("(q %s)", paramsStr != null ? String.format("[%s %s]", query, paramsStr) : query);
        }
    }

    static Query q(QueryStep query, Map<String, Expr> params) {
        return new Query(query, params);
    }

    final class Exists implements Expr {
        public final QueryStep query;
        public final Map<String, Expr> params;

        private Exists(QueryStep query, Map<String, Expr> params) {
            this.query = query;
            this.params = params != null ? Collections.unmodifiableMap(params) : null;
        }

        @Override
        public String toString() {
            String paramsStr = params != null ? params.entrySet().stream().map(e -> String.format("%s %s", e.getKey(), e.getValue())).collect(Collectors.joining(", ")) : null;
            return String.format("(exists? %s)", paramsStr != null ? String.format("[%s %s]", query, paramsStr) : query);
        }
    }

    static Exists exists(QueryStep query, Map<String, Expr> params) {
        return new Exists(query, params);
    }

    final class NotExists implements Expr {
        public final QueryStep query;
        public final Map<String, Expr> params;

        private NotExists(QueryStep query, Map<String, Expr> params) {
            this.query = query;
            this.params = params != null ? Collections.unmodifiableMap(params) : null;
        }

        @Override
        public String toString() {
            String paramsStr = params != null ? params.entrySet().stream().map(e -> String.format("%s %s", e.getKey(), e.getValue())).collect(Collectors.joining(", ")) : null;
            return String.format("(not-exists? %s)", paramsStr != null ? String.format("[%s %s]", query, paramsStr) : query);
        }
    }

    static NotExists notExists(QueryStep query, Map<String, Expr> params) {
        return new NotExists(query, params);
    }

    final class Pull implements Expr {
        public final QueryStep query;
        public final Map<String, Expr> params;

        private Pull(QueryStep query, Map<String, Expr> params) {
            this.query = query;
            this.params = params != null ? Collections.unmodifiableMap(params) : null;
        }

        @Override
        public String toString() {
            String paramsStr = params != null ? params.entrySet().stream().map(e -> String.format("%s %s", e.getKey(), e.getValue())).collect(Collectors.joining(", ")) : null;
            return String.format("(pull %s)", paramsStr != null ? String.format("[%s %s]", query, paramsStr) : query);
        }
    }

    static Pull pull(QueryStep query, Map<String, Expr> params) {
        return new Pull(query, params);
    }

    final class PullMany implements Expr {
        public final QueryStep query;
        public final Map<String, Expr> params;

        private PullMany(QueryStep query, Map<String, Expr> params) {
            this.query = query;
            this.params = params != null ? Collections.unmodifiableMap(params) : null;
        }

        @Override
        public String toString() {
            String paramsStr = params != null ? params.entrySet().stream().map(e -> String.format("%s %s", e.getKey(), e.getValue())).collect(Collectors.joining(", ")) : null;
            return String.format("(pull* %s)", paramsStr != null ? String.format("[%s %s]", query, paramsStr) : query);
        }
    }

    static PullMany pullMany(QueryStep query, Map<String, Expr> params) {
        return new PullMany(query, params);
    }
}
