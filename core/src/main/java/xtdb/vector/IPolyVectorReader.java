package xtdb.vector;

public interface IPolyVectorReader extends IValueReader {
    int valueCount();

    void read(int idx);
}
