package xtdb.util;

import static java.lang.Math.*;

public class TemporalBounds {
    public static class TemporalColumn {
        public long lower = Long.MIN_VALUE;
        public long upper = Long.MAX_VALUE;

        public void lt(long operand) {
            upper = min(operand - 1, upper);
        }

        public void lte(long operand) {
            upper = min(operand, upper);
        }

        public void gt(long operand) {
            lower = max(operand + 1, lower);
        }

        public void gte(long operand) {
            lower = max(operand, lower);
        }

        public boolean inRange(long operand) {
            return lower <= operand && operand <= upper;
        }
    }

    public final TemporalColumn validFrom = new TemporalColumn();
    public final TemporalColumn validTo = new TemporalColumn();
    public final TemporalColumn systemFrom = new TemporalColumn();
    public final TemporalColumn systemTo = new TemporalColumn();

    public boolean inRange(long validFrom, long validTo, long systemFrom, long systemTo) {
        return this.validFrom.inRange(validFrom)
                && this.validTo.inRange(validTo)
                && this.systemFrom.inRange(systemFrom)
                && this.systemTo.inRange(systemTo);
    }
}
