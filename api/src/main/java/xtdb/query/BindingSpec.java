package xtdb.query;

public final class BindingSpec {
    public final String attr;
    public final Expr expr;

    BindingSpec(String attr, Expr expr) {
        this.attr = attr;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return String.format("{%s %s}", attr, expr);
    }

    public static BindingSpec of(String attr, Expr expr) {
        return new BindingSpec(attr, expr);
    }
}
