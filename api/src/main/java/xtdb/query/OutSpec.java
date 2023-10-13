package xtdb.query;

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
}