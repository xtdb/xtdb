package xtdb.types;

import java.time.Duration;
import java.time.Period;

import clojure.lang.PersistentHashMap;

public record IntervalDayTime(Period period, Duration duration) {
    public IntervalDayTime {
        if (period.getYears() != 0 || period.getMonths() != 0)
            throw new xtdb.IllegalArgumentException("Period can not contain years or months!",
                    PersistentHashMap.EMPTY,
                    null);
    }

    @Override
    public String toString() {
        return period.toString() + duration.toString().substring(1);
    }
}
