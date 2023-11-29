package xtdb.types;

import java.time.Period;
import java.util.Objects;

public final class IntervalYearMonth {
    private final Period period;

    public IntervalYearMonth(Period period) {
        this.period = period;
    }

    public Period period() {
        return period;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (IntervalYearMonth) obj;
        return Objects.equals(this.period, that.period);
    }

    @Override
    public int hashCode() {
        return Objects.hash(period);
    }

    @Override
    public String toString() {
        return period.toString();
    }
}
