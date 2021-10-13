package core2.vector;

import org.apache.arrow.vector.ValueVector;

public interface IRowCopier<R extends ValueVector, W extends ValueVector> {
    void copyRow(int sourceIdx);

    IVectorWriter<W> getWriter();
    IIndirectVector<R> getReader();
}
