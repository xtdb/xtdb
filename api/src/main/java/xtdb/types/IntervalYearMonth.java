package xtdb.types;

import java.time.Period;

public record IntervalYearMonth(Period period) {

    @Override
    public String toString() {
        return period.toString();
    }
}
