package xtdb.query;

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
}
