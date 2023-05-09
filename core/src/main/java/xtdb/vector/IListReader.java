package xtdb.vector;

public interface IListReader {

    boolean isPresent(int idx);

    int getElementStartIndex(int idx);

    int getElementEndIndex(int idx);

    IListElementCopier elementCopier(IVectorWriter writer);
}
