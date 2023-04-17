package xtdb.types;

import java.time.Duration;
import java.time.Period;
import java.util.Objects;
import clojure.lang.PersistentHashMap;

public final class IntervalDayTime {
    public final Period period;
    public final Duration duration;

    public IntervalDayTime(Period period, Duration duration) {
        if ( period.getYears() != 0 || period.getMonths() != 0)
            throw new xtdb.IllegalArgumentException("Period can not contain years or months!",
                                                    PersistentHashMap.EMPTY,
                                                    null);
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
        return period.toString() + duration.toString().substring(1);
    }
}
