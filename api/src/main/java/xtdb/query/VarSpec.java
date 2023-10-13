package xtdb.query;

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
}