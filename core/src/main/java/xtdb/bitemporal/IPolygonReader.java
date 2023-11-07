package xtdb.bitemporal;

public interface IPolygonReader {

    int getValidTimeRangeCount();

    long getValidFrom(int rangeIdx);

    long getValidTo(int rangeIdx);

    long getSystemTo(int rangeIdx);
}
