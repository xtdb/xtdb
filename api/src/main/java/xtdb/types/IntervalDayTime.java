package xtdb.types;

import java.time.Duration;
import java.time.Period;
import java.util.Objects;

import clojure.lang.PersistentHashMap;
import xtdb.IllegalArgumentException;

public final class IntervalDayTime {
    private final Period period;
    private final Duration duration;

    public IntervalDayTime(Period period, Duration duration) {
        this.period = period;
        this.duration = duration;

        if (period.getYears() != 0 || period.getMonths() != 0)
            throw new IllegalArgumentException("Period can not contain years or months!",
                    PersistentHashMap.EMPTY,
                    null);
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
        var that = (IntervalDayTime) obj;
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
