package core2.types;

import java.time.Duration;
import java.time.Period;
import java.util.Objects;

public final class IntervalDayTime {
    public final Period period;
    public final Duration duration;

    public IntervalDayTime(Period period, Duration duration) {
        this.period = period;
        this.duration = duration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntervalDayTime that = (IntervalDayTime) o;
        return period.equals(that.period) && duration.equals(that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(period, duration);
    }

    @Override
    public String toString() {
        return String.format("(IntervalDayTime %s %s)", period, duration);
    }
}
