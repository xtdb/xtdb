package xtdb.query;

public interface TemporalFilter {

    interface TemporalExtents extends TemporalFilter {
        Expr from();
        Expr to();
    }

    final class AllTime implements TemporalFilter, TemporalExtents {
        @Override
        public Expr from() {
            return null;
        }

        @Override
        public Expr to() {
            return null;
        }

        @Override
        public String toString() {
            return "all-time";
        }
    }

    TemporalFilter ALL_TIME = new AllTime();

    final class At implements TemporalFilter {
        public final Expr at;

        private At(Expr at) {
            this.at = at;
        }

        @Override
        public String toString() {
            return String.format("(at %s)", at);
        }
    }

    static At at(Expr atExpr) {
        return new At(atExpr);
    }

    final class In implements TemporalFilter, TemporalExtents {
        public final Expr from;
        public final Expr to;

        private In(Expr from, Expr to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public Expr from() {
            return from;
        }

        @Override
        public Expr to() {
            return to;
        }

        @Override
        public String toString() {
            return String.format("(in %s %s)", from, to);
        }
    }

    static In in(Expr fromExpr, Expr toExpr) {
        return new In(fromExpr, toExpr);
    }

    static In from(Expr fromExpr) {
        return new In(fromExpr, null);
    }

    static In to(Expr toExpr) {
        return new In(null, toExpr);
    }
}
