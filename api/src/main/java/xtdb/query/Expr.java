package xtdb.query;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

import java.util.*;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bool bool1 = (Bool) o;
            return bool == bool1.bool;
        }

        @Override
        public int hashCode() {
            return Objects.hash(bool);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Long aLong = (Long) o;
            return lng == aLong.lng;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lng);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Double aDouble = (Double) o;
            return java.lang.Double.compare(dbl, aDouble.dbl) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbl);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Obj obj1 = (Obj) o;
            return Objects.equals(obj, obj1.obj);
        }

        @Override
        public int hashCode() {
            return Objects.hash(obj);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LogicVar logicVar = (LogicVar) o;
            return Objects.equals(lv, logicVar.lv);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lv);
        }
    }

    static LogicVar lVar(String lv) {
        return new LogicVar(lv);
    }

    final class Param implements Expr {
        public final String v;

        private Param(String v) {
            this.v = v;
        }

        @Override
        public String toString() {
            return v;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Param param = (Param) o;
            return Objects.equals(v, param.v);
        }

        @Override
        public int hashCode() {
            return Objects.hash(v);
        }
    }
    static Param param(String v) {
        return new Param(v);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Call call = (Call) o;
            return Objects.equals(f, call.f) && Objects.equals(args, call.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(f, args);
        }
    }

    static Call call(String f, List<Expr> args) {
        return new Call(f, args);
    }

    final class Get implements Expr {
        public final Expr expr;
        public final String field;

        private Get(Expr expr, String field) {
            this.expr = expr;
            this.field = field;
        }

        @Override
        public String toString() {
            return String.format("(. %s %s)", expr, field);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Get get = (Get) o;
            return Objects.equals(expr, get.expr) && Objects.equals(field, get.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expr, field);
        }
    }

    static Get get(Expr expr, String field) {
        return new Get(expr, field);
    }

    final class Subquery implements Expr {
        public final Query query;
        public final List<ArgSpec> args;

        private Subquery(Query query, List<ArgSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(q %s)", stringifyArgs(query, args));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Subquery subquery = (Subquery) o;
            return Objects.equals(query, subquery.query) && Objects.equals(args, subquery.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, args);
        }
    }

    static Subquery q(Query query, List<ArgSpec> args) {
        return new Subquery(query, args);
    }

    final class Exists implements Expr {
        public final Query query;
        public final List<ArgSpec> args;

        private Exists(Query query, List<ArgSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(exists? %s)", stringifyArgs(query, args));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Exists exists = (Exists) o;
            return Objects.equals(query, exists.query) && Objects.equals(args, exists.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, args);
        }
    }

    static Exists exists(Query query, List<ArgSpec> args) {
        return new Exists(query, args);
    }

    final class Vec implements Expr {
        public final List<Expr> elements;

        private Vec(List<Expr> elements) {
            this.elements = elements;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Vec vec = (Vec) o;
            return Objects.equals(elements, vec.elements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(elements);
        }
    }

    static Vec vec(List<Expr> elements){
       return new Vec(elements);
    }

    final class Set implements Expr {
        public final java.util.Set<Expr> elements;

        private Set(java.util.Set<Expr> elements) {
            this.elements = elements;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Set set = (Set) o;
            return Objects.equals(elements, set.elements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(elements);
        }
    }

    static Set set(java.util.Set<Expr> elements){
        return new Set(elements);
    }

    final class Map implements Expr {
        public final java.util.Map<String, Expr> elements;

        public Map(java.util.Map<String, Expr> elements) {
            this.elements = elements;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Map map = (Map) o;
            return Objects.equals(elements, map.elements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(elements);
        }
    }

    static Map map(java.util.Map<String, Expr> elements){
        return new Map(elements);
    }

    final class Pull implements Expr {
        public final Query query;
        public final List<ArgSpec> args;

        private Pull(Query query, List<ArgSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(pull %s)", stringifyArgs(query, args));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pull pull = (Pull) o;
            return Objects.equals(query, pull.query) && Objects.equals(args, pull.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, args);
        }
    }

    static Pull pull(Query query, List<ArgSpec> args) {
        return new Pull(query, args);
    }

    final class PullMany implements Expr {
        public final Query query;
        public final List<ArgSpec> args;

        private PullMany(Query query, List<ArgSpec> args) {
            this.query = query;
            this.args = unmodifiableList(args);
        }

        @Override
        public String toString() {
            return String.format("(pull* %s)", stringifyArgs(query, args));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PullMany pullMany = (PullMany) o;
            return Objects.equals(query, pullMany.query) && Objects.equals(args, pullMany.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, args);
        }
    }

    static PullMany pullMany(Query query, List<ArgSpec> args) {
        return new PullMany(query, args);
    }
}
