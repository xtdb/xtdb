package xtdb.query;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static xtdb.query.QueryUtil.stringifyArgs;
import static xtdb.query.QueryUtil.unmodifiableList;

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

    final class Subquery implements Expr {
        public final Query query;
        public final List<BindingSpec> args;

        private Subquery(Query query, List<BindingSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(q %s)", stringifyArgs(query, args));
        }
    }

    static Subquery q(Query query, List<BindingSpec> args) {
        return new Subquery(query, args);
    }

    final class Exists implements Expr {
        public final Query query;
        public final List<BindingSpec> args;

        private Exists(Query query, List<BindingSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(exists? %s)", stringifyArgs(query, args));
        }
    }

    static Exists exists(Query query, List<BindingSpec> args) {
        return new Exists(query, args);
    }

    final class NotExists implements Expr {
        public final Query query;
        public final List<BindingSpec> args;

        private NotExists(Query query, List<BindingSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(not-exists? %s)", stringifyArgs(query, args));
        }
    }

    static NotExists notExists(Query query, List<BindingSpec> args) {
        return new NotExists(query, args);
    }

    final class Pull implements Expr {
        public final Query query;
        public final List<BindingSpec> args;

        private Pull(Query query, List<BindingSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(pull %s)", stringifyArgs(query, args));
        }
    }

    static Pull pull(Query query, List<BindingSpec> args) {
        return new Pull(query, args);
    }

    final class PullMany implements Expr {
        public final Query query;
        public final List<BindingSpec> args;

        private PullMany(Query query, List<BindingSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(pull* %s)", stringifyArgs(query, args));
        }
    }

    static PullMany pullMany(Query query, List<BindingSpec> args) {
        return new PullMany(query, args);
    }
}
