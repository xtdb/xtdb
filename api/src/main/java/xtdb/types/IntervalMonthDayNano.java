package xtdb.types;

import java.time.Duration;
import java.time.Period;

public record IntervalMonthDayNano(Period period, Duration duration) {

    @Override
    public String toString() {
        return period.toString() + duration.toString().substring(1);
    }
}
