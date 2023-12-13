package xtdb.query;

import java.util.Objects;

public final class ArgSpec {
    public final String attr;
    public final Expr expr;

    ArgSpec(String attr, Expr expr) {
        this.attr = attr;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return String.format("{%s %s}", attr, expr);
    }

    public static ArgSpec of(String attr, Expr expr) {
        return new ArgSpec(attr, expr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArgSpec argSpec = (ArgSpec) o;
        return Objects.equals(attr, argSpec.attr) && Objects.equals(expr, argSpec.expr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attr, expr);
    }
}
