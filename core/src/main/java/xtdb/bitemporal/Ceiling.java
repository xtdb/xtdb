package xtdb.bitemporal;

import com.carrotsearch.hppc.LongArrayList;

import java.util.Arrays;

public record Ceiling(LongArrayList validTimes, LongArrayList sysTimeCeilings) {
    public Ceiling() {
        this(new LongArrayList(), new LongArrayList());
        reset();
    }

    public long getValidFrom(int rangeIdx) {
        return validTimes.get(rangeIdx);
    }

    public long getValidTo(int rangeIdx) {
        return validTimes.get(rangeIdx + 1);
    }

    public long getSystemTime(int rangeIdx) {
        return sysTimeCeilings.get(rangeIdx);
    }

    public void reset() {
        validTimes.clear();
        validTimes.add(Long.MIN_VALUE, Long.MAX_VALUE);

        sysTimeCeilings.clear();
        sysTimeCeilings.add(Long.MAX_VALUE);
    }

    public void applyLog(long systemFrom, long validFrom, long validTo) {
        if (validFrom >= validTo) return;

        var end = Arrays.binarySearch(validTimes.buffer, 0, validTimes.elementsCount, validTo);

        if (end < 0) {
            end = -(end + 1);
            validTimes.insert(end, validTo);
            sysTimeCeilings.insert(end, sysTimeCeilings.get(end - 1));
        }

        var start = Arrays.binarySearch(validTimes.buffer, 0, validTimes.elementsCount, validFrom);
        var insertedStart = start < 0;
        start = insertedStart ? -(start + 1) : start;

        if (insertedStart && start == end) {
            // can't overwrite the value this time as this is already our validTo, so we insert
            validTimes.insert(start, validFrom);
            sysTimeCeilings.insert(start, systemFrom);

            // we've shifted everything one to the right, so increment end
            end++;
        } else {
            validTimes.set(start, validFrom);
            sysTimeCeilings.set(start, systemFrom);
        }

        // delete all the ranges strictly between our start and our end
        if (start + 1 < end) {
            validTimes.removeRange(start + 1, end);
            sysTimeCeilings.removeRange(start + 1, end);
        }
    }
}
