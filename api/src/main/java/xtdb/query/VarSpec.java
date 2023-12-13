package xtdb.query;

import java.util.Objects;

public final class VarSpec {
    public final String attr;
    public final Expr expr;

    VarSpec(String attr, Expr expr) {
        this.attr = attr;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return String.format("{%s %s}", attr, expr);
    }

    public static VarSpec of(String attr, Expr expr) {
        return new VarSpec(attr, expr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VarSpec varSpec = (VarSpec) o;
        return Objects.equals(attr, varSpec.attr) && Objects.equals(expr, varSpec.expr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attr, expr);
    }
}