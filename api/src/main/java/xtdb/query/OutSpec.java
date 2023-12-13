package xtdb.query;

import java.util.Objects;

public final class OutSpec {
    public final String attr;
    public final Expr expr;

    OutSpec(String attr, Expr expr) {
        this.attr = attr;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return String.format("{%s %s}", attr, expr);
    }

    public static OutSpec of(String attr, Expr expr) {
        return new OutSpec(attr, expr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutSpec outSpec = (OutSpec) o;
        return Objects.equals(attr, outSpec.attr) && Objects.equals(expr, outSpec.expr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attr, expr);
    }
}