package xtdb.query;

import java.time.Instant;

public interface TemporalFilter {

    final class AllTime implements TemporalFilter {
        @Override
        public String toString() {
            return "all-time";
        }
    }

    TemporalFilter ALL_TIME = new AllTime();

    final class At implements TemporalFilter {
        public final Instant at;

        private At(Instant at) {
            this.at = at;
        }

        @Override
        public String toString() {
            return String.format("(at %s)", at);
        }
    }

    static At at(Instant atTime) {
        return new At(atTime);
    }

    final class In implements TemporalFilter {
        public final Instant from;
        public final Instant to;

        private In(Instant from, Instant to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public String toString() {
            return String.format("(in %s %s)", from, to);
        }
    }

    static In in(Instant fromTime, Instant toTime) {
        return new In(fromTime, toTime);
    }

    static In from(Instant fromTime) {
        return new In(fromTime, null);
    }

    static In to(Instant toTime) {
        return new In(null, toTime);
    }
}
