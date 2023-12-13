package xtdb.query;

import java.util.Objects;

public final class ColSpec {
    public final String attr;
    public final Expr expr;

    ColSpec(String attr, Expr expr) {
        this.attr = attr;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return String.format("{:%s %s}", attr, expr);
    }

    public static ColSpec of(String attr, Expr expr) {
        return new ColSpec(attr, expr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColSpec colSpec = (ColSpec) o;
        return Objects.equals(attr, colSpec.attr) && Objects.equals(expr, colSpec.expr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attr, expr);
    }
}