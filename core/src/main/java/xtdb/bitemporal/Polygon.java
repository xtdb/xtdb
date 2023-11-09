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

    public void calculateFor(Ceiling ceiling, IPolygonReader inPolygon) {
        validTimes.clear();
        sysTimeCeilings.clear();

        var ceilIdx = 0;

        var validTimeRangeCount = inPolygon.getValidTimeRangeCount();
        assert validTimeRangeCount > 0;

        int rangeIdx = 0;

        var validTime = inPolygon.getValidFrom(rangeIdx);
        var validTo = inPolygon.getValidTo(rangeIdx);
        var systemTo = inPolygon.getSystemTo(rangeIdx);

        while (true) {
            if (systemTo != Long.MAX_VALUE) {
                validTimes.add(validTime);
                sysTimeCeilings.add(systemTo);
                validTime = validTo;
            } else {
                long ceilValidTo;

                while(true) {
                    ceilValidTo = ceiling.getValidTo(ceilIdx);
                    if (ceilValidTo > validTime) break;
                    ceilIdx++;
                }

                validTimes.add(validTime);
                sysTimeCeilings.add(ceiling.getSystemTime(ceilIdx));

                validTime = Math.min(ceilValidTo, validTo);
            }

            if (validTime == validTo) {
                if (++rangeIdx == validTimeRangeCount) break;

                validTo = inPolygon.getValidTo(rangeIdx);
                systemTo = inPolygon.getSystemTo(rangeIdx);
            }
        }

        validTimes.add(validTime);
    }
}
