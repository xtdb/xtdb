package core2.types;

import java.time.Period;
import java.util.Objects;

public final class IntervalYearMonth {
    public final Period period;

    public IntervalYearMonth(Period period) {
        this.period = period;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntervalYearMonth that = (IntervalYearMonth) o;
        return period.equals(that.period);
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
