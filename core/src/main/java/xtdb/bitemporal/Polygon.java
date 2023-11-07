package xtdb.bitemporal;

import com.carrotsearch.hppc.LongArrayList;

public record Polygon(LongArrayList validTimes, LongArrayList sysTimeCeilings) implements IPolygonReader {
    public Polygon() {
        this(new LongArrayList(), new LongArrayList());
    }

    @Override
    public int getValidTimeRangeCount() {
        return sysTimeCeilings.elementsCount;
    }

    @Override
    public long getValidFrom(int rangeIdx) {
        return validTimes.get(rangeIdx);
    }

    @Override
    public long getValidTo(int rangeIdx) {
        return validTimes.get(rangeIdx + 1);
    }

    @Override
    public long getSystemTo(int rangeIdx) {
        return sysTimeCeilings.get(rangeIdx);
    }

    public void calculateFor(Ceiling ceiling, long validFrom, long validTo) {
        validTimes.clear();
        sysTimeCeilings.clear();
        if (validFrom >= validTo)
            throw new IllegalArgumentException("invalid range: %d -> %d".formatted(validFrom, validTo));

        var ceilValidTimes = ceiling.validTimes();
        var ceilSysTimeCeilings = ceiling.sysTimeCeilings();

        // start at 1 because 0 is always `Long.MIN_VALUE`.
        var startAdded = false;
        assert (ceilValidTimes.get(0) == Long.MIN_VALUE);
        for (int i = 1; i < ceilSysTimeCeilings.size(); i++) {
            long validTime = ceilValidTimes.get(i);
            if (validTime < validFrom) continue;

            if (!startAdded) {
                startAdded = true;
                if (validTime != validFrom) {
                    validTimes.add(validFrom);
                    sysTimeCeilings.add(ceilSysTimeCeilings.get(i - 1));
                }
            }

            if (validTime >= validTo) break;

            validTimes.add(validTime);
            sysTimeCeilings.add(ceilSysTimeCeilings.get(i));
        }

        if (!startAdded) {
            validTimes.add(validFrom);
            sysTimeCeilings.add(ceilSysTimeCeilings.get(0));
        }

        validTimes.add(validTo);
    }
}
