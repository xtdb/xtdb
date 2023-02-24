package core2.vector;

public interface IPolyVectorReader extends IPolyValueReader {
    int valueCount();

    byte read(int idx);
}
