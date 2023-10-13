package xtdb.query;

public final class ColSpec {
    public final String attr;
    public final Expr expr;

    ColSpec(String attr, Expr expr) {
        this.attr = attr;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return String.format("{%s %s}", attr, expr);
    }

    public static ColSpec of(String attr, Expr expr) {
        return new ColSpec(attr, expr);
    }
}