package xtdb.types;

import java.time.Duration;
import java.time.Period;
import java.util.Objects;

public final class IntervalMonthDayNano {
    private final Period period;
    private final Duration duration;

    public IntervalMonthDayNano(Period period, Duration duration) {
        this.period = period;
        this.duration = duration;
    }

    public Period period() {
        return period;
    }

    public Duration duration() {
        return duration;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (IntervalMonthDayNano) obj;
        return Objects.equals(this.period, that.period) &&
                Objects.equals(this.duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(period, duration);
    }

    @Override
    public String toString() {
        return period.toString() + duration.toString().substring(1);
    }
}
