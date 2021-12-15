package core2.vector;

public interface IListReader {

    boolean isPresent(int idx);

    IListElementCopier elementCopier(IVectorWriter<?> writer);
}
